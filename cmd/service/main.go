// Command service is the HTTP API that runs on ECS Fargate behind the ALB.
//
//	GET  /health            200 "ok" (ALB health check)
//	GET  /                  service info (version, task hostname)
//	POST /orders            publishes an OrderPlaced event to SNS, returns 202 + eventId
//	GET  /orders/{eventId}  shows what the consumer recorded for that event
//
// A client may send an Idempotency-Key header on POST /orders; it becomes the
// eventId, so a retried request produces the same event and the consumer
// processes it only once.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type orderRequest struct {
	OrderID         string  `json:"orderId"`
	Amount          float64 `json:"amount"`
	Customer        string  `json:"customer,omitempty"`
	SimulateFailure bool    `json:"simulateFailure,omitempty"`
}

// orderPlaced is the domain event published to SNS.
type orderPlaced struct {
	Type            string  `json:"type"`
	EventID         string  `json:"eventId"`
	OrderID         string  `json:"orderId"`
	Amount          float64 `json:"amount"`
	Customer        string  `json:"customer,omitempty"`
	SimulateFailure bool    `json:"simulateFailure,omitempty"`
	OccurredAt      string  `json:"occurredAt"`
}

// publisher and itemGetter are the parts of the SNS and DynamoDB clients the
// server uses; tests replace them with fakes.
type publisher interface {
	Publish(ctx context.Context, in *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type itemGetter interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

type server struct {
	sns      publisher
	db       itemGetter
	topicArn string
	table    string
	version  string
	host     string
	log      *slog.Logger
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Error("load aws config", "error", err.Error())
		os.Exit(1)
	}
	host, _ := os.Hostname()
	s := &server{
		sns:      sns.NewFromConfig(cfg),
		db:       dynamodb.NewFromConfig(cfg),
		topicArn: mustEnv("TOPIC_ARN"),
		table:    mustEnv("DEDUPE_TABLE"),
		version:  os.Getenv("APP_VERSION"),
		host:     host,
		log:      log,
	}

	srv := &http.Server{Addr: ":8080", Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}

	// ECS sends SIGTERM on stop; finish in-flight requests before exiting.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Info("listening", "addr", srv.Addr, "version", s.version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "error", err.Error())
		os.Exit(1)
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /{$}", s.info)
	mux.HandleFunc("POST /orders", s.placeOrder)
	mux.HandleFunc("GET /orders/{eventId}", s.getEvent)
	return mux
}

func (s *server) info(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "orders-api",
		"version": s.version,
		"task":    s.host,
	})
}

func (s *server) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req orderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.OrderID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "orderId is required"})
		return
	}

	eventID := r.Header.Get("Idempotency-Key")
	if eventID == "" {
		eventID = newID()
	}
	ev := orderPlaced{
		Type:            "OrderPlaced",
		EventID:         eventID,
		OrderID:         req.OrderID,
		Amount:          req.Amount,
		Customer:        req.Customer,
		SimulateFailure: req.SimulateFailure,
		OccurredAt:      time.Now().UTC().Format(time.RFC3339),
	}
	body, _ := json.Marshal(ev)

	out, err := s.sns.Publish(r.Context(), &sns.PublishInput{
		TopicArn: aws.String(s.topicArn),
		Message:  aws.String(string(body)),
	})
	if err != nil {
		s.log.Error("publish failed", "eventId", eventID, "error", err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not publish event"})
		return
	}
	s.log.Info("event published", "eventId", eventID, "orderId", req.OrderID, "snsMessageId", aws.ToString(out.MessageId))
	writeJSON(w, http.StatusAccepted, map[string]string{"eventId": eventID, "status": "PUBLISHED"})
}

func (s *server) getEvent(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.GetItem(r.Context(), &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            map[string]types.AttributeValue{"eventId": &types.AttributeValueMemberS{Value: r.PathValue("eventId")}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		s.log.Error("get item failed", "error", err.Error())
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not read table"})
		return
	}
	if out.Item == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not processed (yet)"})
		return
	}
	var item map[string]any
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("missing env var", "name", k)
		os.Exit(1)
	}
	return v
}

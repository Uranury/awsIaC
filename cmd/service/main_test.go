package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type fakeSNS struct {
	got *sns.PublishInput
	err error
}

func (f *fakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.got = in
	if f.err != nil {
		return nil, f.err
	}
	return &sns.PublishOutput{MessageId: aws.String("msg-1")}, nil
}

type fakeDB struct {
	item map[string]types.AttributeValue
	err  error
}

func (f *fakeDB) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.GetItemOutput{Item: f.item}, nil
}

func newTestServer(p *fakeSNS, db *fakeDB) http.Handler {
	s := &server{
		sns:      p,
		db:       db,
		topicArn: "arn:aws:sns:eu-north-1:000000000000:test",
		table:    "test",
		version:  "test/abc123",
		host:     "task-1",
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return s.routes()
}

func do(h http.Handler, method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	rec := do(newTestServer(&fakeSNS{}, &fakeDB{}), http.MethodGet, "/health", "", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "broken" {
		t.Fatalf("GET /health = %d %q, want 200 \"ok\"", rec.Code, rec.Body.String())
	}
}

func TestInfoReportsVersion(t *testing.T) {
	rec := do(newTestServer(&fakeSNS{}, &fakeDB{}), http.MethodGet, "/", "", nil)
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("GET / body is not JSON: %v", err)
	}
	if rec.Code != http.StatusOK || body["version"] != "test/abc123" || body["task"] != "task-1" {
		t.Fatalf("GET / = %d %v", rec.Code, body)
	}
}

func TestPlaceOrderRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"invalid JSON":    `{not json`,
		"missing orderId": `{"amount": 10}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			p := &fakeSNS{}
			rec := do(newTestServer(p, &fakeDB{}), http.MethodPost, "/orders", body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if p.got != nil {
				t.Fatal("published an event for a rejected request")
			}
		})
	}
}

func TestPlaceOrderUsesIdempotencyKey(t *testing.T) {
	p := &fakeSNS{}
	rec := do(newTestServer(p, &fakeDB{}), http.MethodPost, "/orders",
		`{"orderId":"o-1","amount":42.5}`, map[string]string{"Idempotency-Key": "evt-1"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var ev orderPlaced
	if err := json.Unmarshal([]byte(aws.ToString(p.got.Message)), &ev); err != nil {
		t.Fatalf("published message is not JSON: %v", err)
	}
	if ev.Type != "OrderPlaced" || ev.EventID != "evt-1" || ev.OrderID != "o-1" || ev.Amount != 42.5 {
		t.Fatalf("published event = %+v", ev)
	}
	if aws.ToString(p.got.TopicArn) != "arn:aws:sns:eu-north-1:000000000000:test" {
		t.Fatalf("published to %q", aws.ToString(p.got.TopicArn))
	}
}

func TestPlaceOrderPublishFailure(t *testing.T) {
	rec := do(newTestServer(&fakeSNS{err: errors.New("boom")}, &fakeDB{}), http.MethodPost, "/orders",
		`{"orderId":"o-1"}`, nil)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestGetEvent(t *testing.T) {
	t.Run("not processed yet", func(t *testing.T) {
		rec := do(newTestServer(&fakeSNS{}, &fakeDB{}), http.MethodGet, "/orders/evt-1", "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})
	t.Run("completed", func(t *testing.T) {
		db := &fakeDB{item: map[string]types.AttributeValue{
			"eventId": &types.AttributeValueMemberS{Value: "evt-1"},
			"status":  &types.AttributeValueMemberS{Value: "COMPLETED"},
		}}
		rec := do(newTestServer(&fakeSNS{}, db), http.MethodGet, "/orders/evt-1", "", nil)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"COMPLETED"`) {
			t.Fatalf("GET /orders/evt-1 = %d %s", rec.Code, rec.Body.String())
		}
	})
}

// Command consumer is the Lambda function behind the SQS event source mapping.
//
// SQS delivers at least once, so the same OrderPlaced event can arrive more
// than once. The consumer makes processing idempotent with the dedupe table:
//
//  1. Claim the eventId with a conditional PutItem (status IN_PROGRESS + lease).
//     If the row is already COMPLETED the message is a duplicate: skip it.
//     If another worker holds a live lease: report failure, SQS retries later.
//  2. Do the work (here: record the order on the same row).
//  3. Mark the row COMPLETED.
//
// On failure the claim is released and only that message is reported back
// (ReportBatchItemFailures). After maxReceiveCount failed receives SQS moves
// it to the dead-letter queue.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	inProgress = "IN_PROGRESS"
	completed  = "COMPLETED"

	// Longer than the queue's retention, so a late duplicate always finds its row.
	dedupeTTL = 7 * 24 * time.Hour
	// Equal to the function timeout: a crashed worker's claim frees itself after this.
	claimLease = 30 * time.Second
)

type orderPlaced struct {
	EventID         string  `json:"eventId"`
	OrderID         string  `json:"orderId"`
	Amount          float64 `json:"amount"`
	Customer        string  `json:"customer"`
	SimulateFailure bool    `json:"simulateFailure"`
}

var errInFlight = errors.New("event is being processed by another worker")

type consumer struct {
	db    *dynamodb.Client
	table string
	log   *slog.Logger
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
	table := os.Getenv("DEDUPE_TABLE")
	if table == "" {
		panic("DEDUPE_TABLE is not set")
	}
	c := &consumer{db: dynamodb.NewFromConfig(cfg), table: table, log: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
	lambda.Start(c.handle)
}

func (c *consumer) handle(ctx context.Context, batch events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, msg := range batch.Records {
		if err := c.process(ctx, msg); err != nil {
			c.log.Error("message failed", "messageId", msg.MessageId,
				"receiveCount", msg.Attributes["ApproximateReceiveCount"], "error", err.Error())
			resp.BatchItemFailures = append(resp.BatchItemFailures,
				events.SQSBatchItemFailure{ItemIdentifier: msg.MessageId})
		}
	}
	c.log.Info("batch done", "size", len(batch.Records), "failed", len(resp.BatchItemFailures))
	return resp, nil
}

func (c *consumer) process(ctx context.Context, msg events.SQSMessage) error {
	var ev orderPlaced
	if err := json.Unmarshal([]byte(msg.Body), &ev); err != nil {
		return fmt.Errorf("bad json: %w", err)
	}
	if ev.EventID == "" || ev.OrderID == "" {
		return errors.New("eventId and orderId are required")
	}

	claimed, err := c.claim(ctx, ev.EventID)
	if err != nil {
		return err
	}
	if !claimed {
		c.log.Info("duplicate skipped", "eventId", ev.EventID, "messageId", msg.MessageId)
		return nil
	}

	if ev.SimulateFailure {
		if err := c.release(ctx, ev.EventID); err != nil {
			c.log.Error("release failed", "eventId", ev.EventID, "error", err.Error())
		}
		return errors.New("simulated failure")
	}

	if err := c.complete(ctx, ev); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	c.log.Info("processed", "eventId", ev.EventID, "orderId", ev.OrderID, "amount", ev.Amount)
	return nil
}

// claim returns true if this worker now owns the event, false if the event was
// already completed, and errInFlight if another worker holds a live lease.
func (c *consumer) claim(ctx context.Context, eventID string) (bool, error) {
	now := time.Now()
	_, err := c.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.table),
		Item: map[string]types.AttributeValue{
			"eventId":     s(eventID),
			"status":      s(inProgress),
			"lockedUntil": n(now.Add(claimLease).Unix()),
			"expiresAt":   n(now.Add(dedupeTTL).Unix()),
		},
		ConditionExpression:      aws.String("attribute_not_exists(eventId) OR (#st = :inprog AND lockedUntil < :now)"),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inprog": s(inProgress),
			":now":    n(now.Unix()),
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return true, nil
	}
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return false, fmt.Errorf("claim: %w", err)
	}
	if st, ok := ccf.Item["status"].(*types.AttributeValueMemberS); ok && st.Value == completed {
		return false, nil
	}
	return false, errInFlight
}

// complete records the order on the dedupe row and marks it COMPLETED.
func (c *consumer) complete(ctx context.Context, ev orderPlaced) error {
	_, err := c.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(c.table),
		Key:              map[string]types.AttributeValue{"eventId": s(ev.EventID)},
		UpdateExpression: aws.String("SET #st = :done, orderId = :oid, amount = :amt, customer = :cus, processedAt = :at REMOVE lockedUntil"),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":done": s(completed),
			":oid":  s(ev.OrderID),
			":amt":  &types.AttributeValueMemberN{Value: strconv.FormatFloat(ev.Amount, 'f', -1, 64)},
			":cus":  s(ev.Customer),
			":at":   s(time.Now().UTC().Format(time.RFC3339)),
		},
	})
	return err
}

func (c *consumer) release(ctx context.Context, eventID string) error {
	_, err := c.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String(c.table),
		Key:                       map[string]types.AttributeValue{"eventId": s(eventID)},
		ConditionExpression:       aws.String("#st = :inprog"),
		ExpressionAttributeNames:  map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":inprog": s(inProgress)},
	})
	return err
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

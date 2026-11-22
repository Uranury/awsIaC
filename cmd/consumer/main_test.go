package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeStore keeps the dedupe rows in memory and evaluates the one condition
// that matters for these tests: a row that already exists blocks a new claim.
type fakeStore struct {
	rows    map[string]string // eventId -> status
	deletes int
}

func (f *fakeStore) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	id := in.Item["eventId"].(*types.AttributeValueMemberS).Value
	if st, ok := f.rows[id]; ok {
		return nil, &types.ConditionalCheckFailedException{
			Item: map[string]types.AttributeValue{"status": &types.AttributeValueMemberS{Value: st}},
		}
	}
	f.rows[id] = in.Item["status"].(*types.AttributeValueMemberS).Value
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeStore) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	id := in.Key["eventId"].(*types.AttributeValueMemberS).Value
	f.rows[id] = in.ExpressionAttributeValues[":done"].(*types.AttributeValueMemberS).Value
	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *fakeStore) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	delete(f.rows, in.Key["eventId"].(*types.AttributeValueMemberS).Value)
	f.deletes++
	return &dynamodb.DeleteItemOutput{}, nil
}

func newTestConsumer() (*consumer, *fakeStore) {
	store := &fakeStore{rows: map[string]string{}}
	return &consumer{db: store, table: "test", log: slog.New(slog.NewTextHandler(io.Discard, nil))}, store
}

func batch(bodies ...string) events.SQSEvent {
	var e events.SQSEvent
	for i, b := range bodies {
		e.Records = append(e.Records, events.SQSMessage{MessageId: string(rune('a' + i)), Body: b})
	}
	return e
}

func failedIDs(r events.SQSEventResponse) []string {
	var ids []string
	for _, f := range r.BatchItemFailures {
		ids = append(ids, f.ItemIdentifier)
	}
	return ids
}

func TestDuplicateIsProcessedOnce(t *testing.T) {
	c, store := newTestConsumer()
	msg := `{"type":"OrderPlaced","eventId":"evt-1","orderId":"o-1","amount":42.5}`

	resp, err := c.handle(context.Background(), batch(msg, msg))
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("handle = %v, failures %v; want no failures", err, failedIDs(resp))
	}
	if store.rows["evt-1"] != completed {
		t.Fatalf("row status = %q, want %q", store.rows["evt-1"], completed)
	}
}

func TestOnlyFailedMessagesAreReported(t *testing.T) {
	c, store := newTestConsumer()
	resp, err := c.handle(context.Background(), batch(
		`{"eventId":"evt-ok","orderId":"o-1"}`,
		`{"eventId":"evt-bad","orderId":"o-2","simulateFailure":true}`,
		`not json`,
		`{"orderId":"o-3"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	got := failedIDs(resp)
	want := []string{"b", "c", "d"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("failed message IDs = %v, want %v", got, want)
	}
	if store.rows["evt-ok"] != completed {
		t.Fatalf("evt-ok status = %q, want %q", store.rows["evt-ok"], completed)
	}
	if _, ok := store.rows["evt-bad"]; ok || store.deletes != 1 {
		t.Fatalf("simulated failure did not release its claim (rows %v, deletes %d)", store.rows, store.deletes)
	}
}

func TestLiveClaimIsRetriedLater(t *testing.T) {
	c, store := newTestConsumer()
	store.rows["evt-1"] = inProgress // another worker holds the lease

	resp, err := c.handle(context.Background(), batch(`{"eventId":"evt-1","orderId":"o-1"}`))
	if err != nil || len(resp.BatchItemFailures) != 1 {
		t.Fatalf("handle = %v, failures %v; want the message reported for retry", err, failedIDs(resp))
	}
}

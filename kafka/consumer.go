package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

const PaymentCompletedTopic = "payment-completed"

const PaymentStatusSuccess = "SUCCESS"

type PaymentCompletedEvent struct {
	EventID   string  `json:"event_id"`
	EventType string  `json:"event_type"`
	OrderID   string  `json:"order_id"`
	UserID    int     `json:"user_id"`
	PaymentID string  `json:"payment_id"`
	Status    string  `json:"status"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

func NewConsumer() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup("order-service-group"),
		kgo.ConsumeTopics(PaymentCompletedTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
	)
}

func ConsumePaymentCompleted(
	ctx context.Context,
	client *kgo.Client,
	handle func(context.Context, PaymentCompletedEvent) error,
) {
	for {
		fetches := client.PollFetches(ctx)

		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("Kafka error: topic=%s partition=%d err=%v",
				topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			var event PaymentCompletedEvent

			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("Invalid payment event: %v", err)
				if err := client.CommitRecords(ctx, record); err != nil {
					log.Printf("Commit failed: %v", err)
				}
				return
			}

			log.Printf(
				"Received payment event: order=%s status=%s payment=%s",
				event.OrderID, event.Status, event.PaymentID,
			)

			if err := handle(ctx, event); err != nil {
				log.Printf("Failed to process payment event for order %s: %v", event.OrderID, err)
				return
			}

			if err := client.CommitRecords(ctx, record); err != nil {
				log.Printf("Commit failed: %v", err)
			}
		})
	}
}

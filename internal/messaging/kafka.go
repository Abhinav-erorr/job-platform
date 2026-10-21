package messaging

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(address, topic string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(address),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *Publisher) Publish(ctx context.Context, jobID string) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(jobID),
		Value: []byte(jobID),
	})
}

func (x *Publisher) Close() error {
	return x.writer.Close()
}

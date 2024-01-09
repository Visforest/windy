package main

import (
	"context"
	"github.com/visforest/windy/example"
	"github.com/visforest/windy/kq"
)

func main() {
	cfg := kq.Conf{
		Brokers:    []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:      "notify.email",
		Group:      "g.notify.email",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := kq.MustNewConsumer(&cfg, example.SendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&example.MyConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}

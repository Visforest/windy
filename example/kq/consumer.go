package main

import (
	"context"
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

func main() {
	cfg := windy.KConf{
		Kafka: &windy.KafkaConf{
			Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
			Group:   "g.notify.email",
		},
		Topic:      "notify.email",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := windy.MustNewKConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}

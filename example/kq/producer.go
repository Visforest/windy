package main

import (
	"context"
	"fmt"
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
		Topic: "notify.email",
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := windy.MustNewKProducer(&cfg, core.WithProducerContext(ctx), core.WithProducerListener(&example.MyProduceListener{}), core.WithIdCreator(&example.MyIdCreator{}))

	for _, email := range example.Emails {
		msgId, err := producer.Send(email)
		if err != nil {
			panic(err)
		}
		fmt.Printf("send msg %s \n", msgId)
	}
}

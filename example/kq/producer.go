package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy/example"
	"github.com/visforest/windy/kq"
	"strings"
)

func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := kq.MustNewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&example.MyProduceListener{}), kq.WithIdCreator(&example.MyIdCreator{}))

	receivers := []string{"wind@example.com", "cloud@example.com", "rain@example.com", "snow@example.com", "storm@example.com"}
	for _, r := range receivers {
		data := map[string]string{"receiver": r, "content": fmt.Sprintf("Hi, %s!", strings.TrimRight(r, "@example.com"))}
		msgId, err := producer.Send(data)
		if err != nil {
			panic(err)
		}
		fmt.Printf("send msg %s to %s \n", msgId, r)
	}
}

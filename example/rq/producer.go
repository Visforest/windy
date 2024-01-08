package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy/example"
	"github.com/visforest/windy/rq"
	"strings"
)

func main() {
	cfg := rq.Conf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := rq.MustNewProducer(&cfg, rq.WithProducerContext(ctx), rq.WithProducerListener(&example.MyProduceListener{}))

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

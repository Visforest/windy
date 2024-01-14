package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy/core"

	"github.com/visforest/windy"
	"github.com/visforest/windy/example"
)

func main() {
	cfg := windy.RConf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := windy.MustNewRProducer(&cfg, core.WithProducerContext(ctx), core.WithProducerListener(&example.MyProduceListener{}))

	for _, e := range example.Emails {
		msgId, err := producer.Send(e)
		if err != nil {
			panic(err)
		}
		fmt.Printf("send msg %s \n", msgId)
	}
}

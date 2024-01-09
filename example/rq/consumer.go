package main

import (
	"context"
	"fmt"

	"github.com/visforest/windy/example"
	"github.com/visforest/windy/rq"
)

func main() {
	cfg := rq.Conf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := rq.MustNewConsumer(&cfg, example.SendEmail, rq.WithConsumerContext(ctx), rq.WithConsumerListener(&example.MyConsumerListener{}))
	fmt.Println("start to consume")
	// block to consume
	consumer.LoopConsume()
}

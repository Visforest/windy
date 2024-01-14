package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

func main() {
	cfg := windy.RConf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	fmt.Println("start to consume")
	// block to consume
	consumer.LoopConsume()
}

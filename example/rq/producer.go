package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy/core"

	"github.com/visforest/windy"
	"github.com/visforest/windy/example"
)

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
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

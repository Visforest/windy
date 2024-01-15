package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

func main() {
	var cfg windy.KConf
	windy.MustLoadConfig("config.json", &cfg)
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

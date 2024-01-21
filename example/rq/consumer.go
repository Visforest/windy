package main

import (
	"fmt"

	"github.com/visforest/windy"
	"github.com/visforest/windy/example"
)

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	// ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	// consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	consumer := windy.MustNewRConsumer(&cfg, example.Print)
	fmt.Println("start to consume")
	// block to consume
	consumer.LoopConsume()
}

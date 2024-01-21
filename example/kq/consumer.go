package main

import (
	"github.com/visforest/windy"
	"github.com/visforest/windy/example"
)

func main() {
	var cfg windy.KConf
	windy.MustLoadConfig("config.json", &cfg)
	//ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	//consumer := windy.MustNewKConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	consumer := windy.MustNewKConsumer(&cfg, example.Print)
	// block to consume
	consumer.LoopConsume()
}

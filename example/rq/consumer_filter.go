package main

import (
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

func isInBlacklist(receiver string) bool {
	// your some logic here
	return false
}

// filter valid receivers
func filter(msg *core.Msg) bool {
	var data example.Email
	if err := core.ParseFromMsg(msg, &data); err == nil {
		for _, r := range data.Receivers {
			if isInBlacklist(r) {
				return false
			}
		}
		if isInBlacklist(data.Receiver) {
			return false
		}
	}
	return true
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithFilterFunc(filter))
	consumer.LoopConsume()
}

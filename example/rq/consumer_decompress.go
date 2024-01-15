package main

import (
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

// defines the receivers of email should be single
func decompress(msg *core.Msg) []*core.Msg {
	var msgs []*core.Msg
	var email example.Email
	if err := core.ParseFromMsg(msg, &email); err != nil {
		panic(err)
	}
	if len(email.Receivers) > 0 {
		// multi receivers
		msgs = make([]*core.Msg, len(email.Receivers))
		for i, receiver := range email.Receivers {
			msgs[i] = core.NewMsg(example.Email{
				Receiver: receiver,
				Subject:  email.Subject,
				Content:  email.Content,
			})
		}
	} else {
		// single receiver
		msgs = []*core.Msg{msg}
	}
	return msgs
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithDecompressFunc(decompress))
	consumer.LoopConsume()
}

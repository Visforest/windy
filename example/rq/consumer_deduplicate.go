package main

import (
	"fmt"
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
	"strings"
)

// defines the combination of receiver and subject must be unique
func uniq(msg *core.Msg) string {
	var data example.Email
	if err := core.ParseFromMsg(msg, &data); err == nil {
		subject := data.Subject
		var receiver string
		if len(data.Receivers) > 0 {
			receiver = strings.Join(data.Receivers, ";")
		} else {
			receiver = data.Receiver
		}
		return fmt.Sprintf("%s:%s", receiver, subject)
	}
	return ""
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithUniqFunc(uniq))
	consumer.LoopConsume()
}

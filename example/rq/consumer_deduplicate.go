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
	cfg := windy.RConf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
		BatchProcessConf: &windy.BatchProcessConf{
			Batch:   5,
			Timeout: 30,
		},
	}
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithUniqFunc(uniq))
	consumer.LoopConsume()
}

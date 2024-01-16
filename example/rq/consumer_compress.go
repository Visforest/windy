package main

import (
	"fmt"
	"github.com/Visforest/goset"
	"github.com/visforest/windy"
	"github.com/visforest/windy/core"
	"github.com/visforest/windy/example"
)

// returns msgs grouped by Email.Subject and Email.Content
func compress(msgs []*core.Msg) []*core.Msg {
	group := make(map[string]*goset.Set)
	group2 := make(map[string]*core.Msg)
	for _, msg := range msgs {
		var email example.Email
		if err := core.ParseFromMsg(msg, &email); err != nil {
			fmt.Printf("%v \n", err)
			continue
		}
		key := fmt.Sprintf("%s_%s", email.Subject, email.Content)
		if email.Receiver != "" {
			email.Receivers = append(email.Receivers, email.Receiver)
		}
		if _, ok := group[key]; !ok {
			group[key] = goset.NewSet()
		}
		for _, r := range email.Receivers {
			group[key].Add(r)
		}
		if _, ok := group2[key]; !ok {
			group2[key] = &core.Msg{
				Id: msg.Id,
				Data: example.Email{
					Subject: email.Subject,
					Content: email.Content,
				},
			}
		}
	}
	var result = make([]*core.Msg, 0, len(group))
	for key, receiverSet := range group {
		// get receivers
		receivers := make([]string, 0, receiverSet.Length())
		for _, r := range receiverSet.ToList() {
			receivers = append(receivers, r.(string))
		}
		// get email
		msg := group2[key]
		email := msg.Data.(example.Email)
		email.Receivers = receivers
		msg.Data = email
		result = append(result, msg)
	}
	return result
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.BatchSendEmail, core.WithCompressFunc(compress))
	consumer.LoopConsume()
}

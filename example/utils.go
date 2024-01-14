package example

import (
	"context"
	"errors"
	"fmt"
	"github.com/visforest/windy/core"
	"math/rand"
	"strings"
)

type Email struct {
	Receivers []string
	Receiver  string
	Subject   string
	Content   string
}

var Emails = []Email{
	{
		Receiver: "wind@example.com",
		Subject:  "Award!",
		Content:  "You got a $15 award.",
	},
	{
		Receiver: "rain@example.com",
		Subject:  "Award!",
		Content:  "You got a $15 award.",
	},
	{
		Receiver: "wind@example.com",
		Subject:  "Award!",
		Content:  "You got a $20 award.",
	},
	{
		Receivers: []string{"snow@example.com", "cloud@example.com"},
		Subject:   "Join your 2024 contest",
		Content:   "Making new friends? Looking for a new job? Join the 2024 algorithm contest with over 200 coders!",
	},
	{
		Receiver: "cloud@example.com",
		Subject:  "New message from your friend",
		Content:  "Your friend Susan sent a message minutes ago.",
	},
	{
		Receiver: "storm@example.com",
		Subject:  "%70 discount!",
		Content:  "Upgrade to VIP, and you're going to have 70% discount every Friday!",
	},
}

type MyProduceListener struct{}

func (l *MyProduceListener) PrepareSend(ctx context.Context, topic string, msg *core.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before msg is sent:%s \n", err.Error())
		return
	}
	channel := ctx.Value("channel").(string)
	fmt.Printf("prepare to send msg %s to %s,channel: %s\n", msg.Id, topic, channel)
}

func (l *MyProduceListener) OnSendSucceed(ctx context.Context, topic string, msg *core.Msg) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("sent msg %s to %s,channel: %s \n", msg.Id, topic, channel)
}

func (l *MyProduceListener) OnSendFail(ctx context.Context, topic string, msg *core.Msg, err error) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("failed to send msg %s to %s, %s,channel: %s \n", msg.Id, topic, err.Error(), channel)
}

type MyConsumerListener struct{}

func (l *MyConsumerListener) PrepareConsume(ctx context.Context, topic string, msg *core.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before consume:%s \n", err.Error())
		return
	}
	ip := ctx.Value("myip").(string)
	fmt.Printf("consuming msg %s from %s,ip: %s\n", msg.Id, topic, ip)
}

func (l *MyConsumerListener) OnConsumeSucceed(ctx context.Context, topic string, msg *core.Msg) {
	ip := ctx.Value("myip").(string)
	fmt.Printf("consume msg %s from %s,ip: %s \n", msg.Id, topic, ip)
}

func (l *MyConsumerListener) OnConsumeFail(ctx context.Context, topic string, msg *core.Msg, err error) {
	ip := ctx.Value("myip").(string)
	fmt.Printf("failed to consume msg %s from %s, %s,ip: %s \n", msg.Id, topic, err.Error(), ip)
}

// MyIdCreator is a customized id creator
type MyIdCreator struct{}

func (c *MyIdCreator) Create() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 10)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func SendEmail(ctx context.Context, topic string, msg *core.Msg) error {
	ip, ok := ctx.Value("myip").(string)
	if ok {
		fmt.Printf("start to send email from ip: %s,topic is %s \n", ip, topic)
	} else {
		fmt.Printf("start to send email,topic is %s \n", topic)
	}
	var email Email
	if err := core.ParseFromMsg(msg, &email); err == nil {
		fmt.Printf("send to %s:%s \n", email.Receiver, email.Content)
		return nil
	}
	return errors.New("got bad data from queue")
}

func BatchSendEmail(ctx context.Context, topic string, msg *core.Msg) error {
	ip, ok := ctx.Value("myip").(string)
	if ok {
		fmt.Printf("start to send email from ip: %s,topic is %s \n", ip, topic)
	} else {
		fmt.Printf("start to send email,topic is %s \n", topic)
	}
	var email Email
	if err := core.ParseFromMsg(msg, &email); err == nil {
		fmt.Printf("send to %s:%s \n", strings.Join(email.Receivers, ";"), email.Content)
		return nil
	}
	return errors.New("got bad data from queue")
}

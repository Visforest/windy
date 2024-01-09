package example

import (
	"context"
	"errors"
	"fmt"
	"github.com/visforest/windy/model"
	"math/rand"
)

type MyProduceListener struct{}

func (l *MyProduceListener) PrepareSend(ctx context.Context, topic string, msg *model.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before msg is sent:%s \n", err.Error())
		return
	}
	channel := ctx.Value("channel").(string)
	fmt.Printf("sending msg %s to %s,channel: %s\n", msg.Id, topic, channel)
}

func (l *MyProduceListener) OnSendSucceed(ctx context.Context, topic string, msg *model.Msg) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("sent msg %s to %s,channel: %s \n", msg.Id, topic, channel)
}

func (l *MyProduceListener) OnSendFail(ctx context.Context, topic string, msg *model.Msg, err error) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("failed to send msg %s to %s, %s,channel: %s \n", msg.Id, topic, err.Error(), channel)
}

type MyConsumerListener struct{}

func (l *MyConsumerListener) PrepareConsume(ctx context.Context, topic string, msg *model.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before consume:%s \n", err.Error())
		return
	}
	ip := ctx.Value("myip").(string)
	fmt.Printf("consuming msg %s from %s,ip: %s\n", msg.Id, topic, ip)
}

func (l *MyConsumerListener) OnConsumeSucceed(ctx context.Context, topic string, msg *model.Msg) {
	ip := ctx.Value("myip").(string)
	fmt.Printf("consume msg %s from %s,ip: %s \n", msg.Id, topic, ip)
}

func (l *MyConsumerListener) OnConsumeFail(ctx context.Context, topic string, msg *model.Msg, err error) {
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

func SendEmail(ctx context.Context, topic string, msg *model.Msg) error {
	ip := ctx.Value("myip").(string)
	fmt.Printf("start to send email from ip: %s,topic is %s \n", ip, topic)
	if data, ok := msg.Data.(map[string]interface{}); ok {
		receiver, ok := data["receiver"]
		if !ok {
			return errors.New("got bad data from queue")
		}
		content, ok := data["content"]
		if !ok {
			return errors.New("got bad data from queue")
		}
		fmt.Printf("send to %s:%s \n", receiver, content)
		return nil
	}
	return errors.New("got bad data from queue")
}

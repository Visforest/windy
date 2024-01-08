package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/visforest/windy/kq"
	"github.com/visforest/windy/model"
)

type myConsumerListener struct{}

func (l *myConsumerListener) PrepareConsume(ctx context.Context, topic string, msg *model.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before consume:%s \n", err.Error())
		return
	}
	ip := ctx.Value("myip").(string)
	fmt.Printf("consuming msg %s from %s,ip: %s\n", msg.Id, topic, ip)
}

func (l *myConsumerListener) OnConsumeSucceed(ctx context.Context, topic string, msg *model.Msg) {
	ip := ctx.Value("myip").(string)
	fmt.Printf("consume msg %s from %s,ip: %s \n", msg.Id, topic, ip)
}

func (l *myConsumerListener) OnConsumeFail(ctx context.Context, topic string, msg *model.Msg, err error) {
	ip := ctx.Value("myip").(string)
	fmt.Printf("failed to consume msg %s from %s, %s,ip: %s \n", msg.Id, topic, err.Error(), ip)
}

func sendEmail(ctx context.Context, topic string, msg *model.Msg) error {
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

func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := kq.MustNewConsumer(&cfg, sendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&myConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}

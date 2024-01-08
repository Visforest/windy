
English | [中文](README_ZH.md)

# windy
A Go library for queueing message/task and processing them asynchronously. It's based on Kafka for now.

Supports:
1. call customized hook function before,after sending msg and on failing to send msg. 
2. customized msg id generator
3. use Context so that you can pass and use metadata

Other features are coming soon.

# Usage

Install:
```
go get github.com/visforest/windy
```

Producer:
```go
func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
	}
	// pass metadata
	ctx := context.WithValue(context.Background(), "channel", "pc")
	// initialize a producer
	// optionally specify your listener and id creator
	producer := kq.NewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&myProduceListener{}), kq.WithIdCreator(&myIdCreator{}))
    
	// prepare and send msgs
	receivers := []string{"wind@example.com", "cloud@example.com", "rain@example.com", "snow@example.com", "storm@example.com"}
	for _, r := range receivers {
		data := map[string]string{"receiver": r, "content": fmt.Sprintf("Hi, %s!", strings.TrimRight(r, "@example.com"))}
		msgId, err := producer.Send(data)
		if err != nil {
			panic(err)
		}
		fmt.Printf("send msg %s to %s \n", msgId, r)
	}
}
```

You can define your listener and id creator, see [example/producer.go](example/producer.go).

Consumer:
```go
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
	// pass metadata
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	// initialize a consumer
	consumer := kq.MustNewConsumer(&cfg, sendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&myConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}
```
You can define your listener,see [example/consumer.go](example/consumer.go).
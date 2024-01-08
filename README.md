
English | [中文](README_ZH.md)

# windy
A Go library for queueing message/task and processing them asynchronously. `kq` based on Kafka and `rq` based on Redis are supplied for now.

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

Below is the usage of `kq`, and `rq` has the same usage.

Producer:
```go
func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := kq.MustNewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&example.MyProduceListener{}), kq.WithIdCreator(&example.MyIdCreator{}))

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

Consumer:
```go
func main() {
	cfg := kq.Conf{
		Brokers:    []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:      "notify.email",
		Group:      "g.notify.email",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := kq.MustNewConsumer(&cfg, example.SendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&example.MyConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}
```

You may customize your producer listener, consumer listener, msg id creator and the function that handles msgs, see [example/utils.go](example/utils.go) for reference.
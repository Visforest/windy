
English | [中文](README_ZH.md)

# windy
A Go library for queueing message/task and processing them asynchronously. `kq` based on Kafka and `rq` based on Redis are supplied for now.

Supports:
1. call customized hook function before,after sending msg and on failing to send msg. 
2. customized msg id generator
3. use Context so that you can pass and use metadata
4. compress msgs
5. decompress msgs
6. deduplicate msgs

Other features are coming soon.

# Usage

Install:
```
go get github.com/visforest/windy
```

Below is the usage of `kq`, and `rq` has the same usage.

## Producer

### normal produce

Context with metadata,IdCreator, ProducerListener are all optional.

```go
func main() {
	cfg := windy.RConf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	fmt.Println("start to consume")
	// block to consume
	consumer.LoopConsume()
}
```
## Consumer

### context,listener

context,listener are optional.

context could carry metadata, and listener could be monitored before consume,after consume succeed and after consume fails, so that you could record logs or handle something.

```go
func main() {
	cfg := windy.RConf{
		Url:        "redis://127.0.0.1:6379",
		Topic:      "notify:email",
		KeyPrefix:  "myapp",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
	fmt.Println("start to consume")
	// block to consume
	consumer.LoopConsume()
}
```

### compress and consume

msgs can be compressed. For example, same emails to different receivers could be simplied to be one email with a group of receivers. 

```go
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
	consumer := windy.MustNewRConsumer(&cfg, example.BatchSendEmail, core.WithCompressFunc(compress))
	consumer.LoopConsume()
}
```

### decompress and consume

Sometimes one msg need to be decompressed into many. Below is an example. 
```go

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
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithDecompressFunc(decompress))
	consumer.LoopConsume()
}
```

### deduplicate and consume

example codes:
```go
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
```

---

You may customize your producer listener, consumer listener, msg id creator and the function that handles msgs, see [example/utils.go](example/utils.go) for reference.
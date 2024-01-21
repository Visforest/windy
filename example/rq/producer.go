package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/visforest/windy/core"

	"github.com/visforest/windy"
	"github.com/visforest/windy/example"
)

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := windy.MustNewRProducer(&cfg, core.WithProducerContext(ctx), core.WithProducerListener(&example.MyProduceListener{}))

	// for _, e := range example.Emails {
	// 	msgId, err := producer.Send(e)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("send msg %s \n", msgId)
	// }

	f, err := os.OpenFile("sent.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for i := 1; i < 10001; i++ {
		producer.Send(i)

		s1 := fmt.Sprintf("send %d \n", i)
		fmt.Printf(s1)
		if _, err := f.WriteString(s1); err != nil {
			panic(err)
		}

		if rand.Intn(3) == 0 {
			producer.Send(i)
			s2 := fmt.Sprintf("send %d again \n", i)
			fmt.Print(s2)
			if _, err := f.WriteString(s2); err != nil {
				panic(err)
			}
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
	}
}

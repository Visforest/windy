package core

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Visforest/goset/v2"
)

// ProcessorType is the type of msg processors
// priority: decompress -> deduplicate -> compress
type ProcessorType uint8

const (
	decompressor ProcessorType = 1
	deduplicator ProcessorType = 2
	compressor   ProcessorType = 3
)

type ConsumeFunc func(ctx context.Context, topic string, msg *Msg) error

// UniqFunc returns a generated unique string that distinguishes from another model.Msg to deduplicate msgs
type UniqFunc func(msg *Msg) string

// DecompressFunc returns decompressed msgs
type DecompressFunc func(msg *Msg) []*Msg

// CompressFunc returns compressed msgs
type CompressFunc func(msgs []*Msg) []*Msg

type ConsumerOption func(consumer *ConsumerCore)

func WithConsumerContext(ctx context.Context) ConsumerOption {
	return func(c *ConsumerCore) {
		c.Ctx = ctx
	}
}

func WithConsumerListener(listener ConsumeListener) ConsumerOption {
	return func(c *ConsumerCore) {
		c.listener = listener
	}
}

func WithUniqFunc(f UniqFunc) ConsumerOption {
	return func(c *ConsumerCore) {
		c.uniq = f
		c.Processors.Add(deduplicator)
	}
}

func WithDecompressFunc(f DecompressFunc) ConsumerOption {
	return func(c *ConsumerCore) {
		c.decompress = f
		c.Processors.Add(decompressor)
	}
}

func WithCompressFunc(f CompressFunc) ConsumerOption {
	return func(c *ConsumerCore) {
		c.compress = f
		c.Processors.Add(compressor)
	}
}

type ConsumerCore struct {
	Ctx                 context.Context // required
	Topic               string          // required
	WorkersNum          int             // required
	ConsumeFunc         ConsumeFunc     // required
	listener            ConsumeListener // optional
	BatchProcessCnt     int             // optional
	BatchProcessTimeout time.Duration   // optional

	// since function is not comparable and cannot be used at goset.FifoSet, use enum instead.
	// processors records all the msg process function in sequence of priority.
	Processors *goset.SortedSet[ProcessorType]
	uniq       UniqFunc       // optional
	decompress DecompressFunc // optional
	compress   CompressFunc   // optional
}

// fetch msgs in one fetch cycle
func (c *ConsumerCore) fetchBatchMsgs(consumer consumer, chOut chan<- *Msg) {
	finish := false
	chMsg := make(chan *Msg, 1)

	var wg sync.WaitGroup
	wg.Add(2)

	// fetch msgs
	go func() {
		defer wg.Done()
		for !finish {
			m, err := consumer.Fetch()
			if c.listener != nil {
				c.listener.PrepareConsume(c.Ctx, c.Topic, m, err)
			}
			if err != nil {
				// fail, skip
				fmt.Println("fetchBatchMsgs err:", err)
				continue
			}
			chMsg <- m
		}
		close(chMsg)
	}()

	go func() {
		defer wg.Done()
		// collect msgs util enough or timed out
		fetched := 0
		for fetched < c.BatchProcessCnt {
			select {
			case <-time.After(c.BatchProcessTimeout):
				finish = true
				return
			case m, ok := <-chMsg:
				if !ok {
					return
				}
				chOut <- m
				fetched++
			}
		}
		finish = true
	}()

	wg.Wait()
}

func (c *ConsumerCore) fetchMany(consumer consumer, chOut chan<- *Msg) {
	if c.Processors.Has(compressor) || c.Processors.Has(deduplicator) {
		// loop batch fetch msgs in limited count or limited time
		for {
			c.fetchBatchMsgs(consumer, chOut)
		}
	} else {
		// loop fetch msg
		for {
			m, err := consumer.Fetch()
			if c.listener != nil {
				c.listener.PrepareConsume(c.Ctx, c.Topic, m, err)
			}
			if err != nil {
				// fail, skip
				fmt.Println("fetchMany err:", err)
				continue
			}
			chOut <- m
		}
	}
}

// fetch msgs from chIn,deduplicate,and then sent to chOut
func (c *ConsumerCore) deduplicateMsg(chIn <-chan *Msg, chOut chan<- *Msg) {
	for {
		fmt.Println("new loop deduplicateMsg")
		var msgs = make([]*Msg, 0, c.BatchProcessCnt)
		for len(msgs) < c.BatchProcessCnt {
			select {
			case m := <-chIn:
				msgs = append(msgs, m)
			case <-time.After(c.BatchProcessTimeout):
				goto deduplicate
			}
		}
	deduplicate:
		seen := goset.NewStrSet()
		for _, m := range msgs {
			id := c.uniq(m)
			if !seen.Has(id) {
				seen.Add(id)
				chOut <- m
			}
		}
	}
}

// fetch msgs from chIn, decompress, and then sent to chOut
func (c *ConsumerCore) decompressMsg(chIn <-chan *Msg, chOut chan<- *Msg) {
	for msg := range chIn {
		msgs := c.decompress(msg)
		for _, m := range msgs {
			chOut <- m
		}
	}
}

// fetch msgs from chIn, compress and then sent to chOut
func (c *ConsumerCore) compressMsg(chIn <-chan *Msg, chOut chan<- *Msg) {
	for {
		var msgs []*Msg
		for msg := range chIn {
			msgs = append(msgs, msg)
			if len(msgs) == c.BatchProcessCnt {
				break
			}
		}
		// compress
		for _, m := range c.compress(msgs) {
			chOut <- m
		}
	}
}

// LoopConsume blocks and consumes msgs in loop with multi goroutine
func (c *ConsumerCore) LoopConsume(consumer consumer) {
	fmt.Println("start consume")
	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	// fetch msgs
	chIn := make(chan *Msg, 1024)
	chOut := make(chan *Msg, 1024)

	// fetch msgs
	go c.fetchMany(consumer, chIn)
	if c.Processors.Length() > 0 {
		// process msgs
		for _, pType := range c.Processors.ToList() {
			switch pType {
			case decompressor:
				go c.decompressMsg(chIn, chOut)
			case deduplicator:
				go c.deduplicateMsg(chIn, chOut)
			case compressor:
				go c.compressMsg(chIn, chOut)
			}
		}
	} else {
		chOut = chIn
	}

	// consume msgs in multi goroutines
	for i := 0; i < c.WorkersNum; i++ {
		go func(no int) {
			for {
				msg := <-chOut
				if c.listener == nil {
					// _ = c.ConsumeFunc(c.Ctx, c.Topic, msg)
					_ = c.ConsumeFunc(c.Ctx, fmt.Sprintf("%d", no), msg)
				} else {
					c.listener.PrepareConsume(c.Ctx, c.Topic, msg, nil)
					if err := c.ConsumeFunc(c.Ctx, c.Topic, msg); err == nil {
						c.listener.OnConsumeSucceed(c.Ctx, c.Topic, msg)
					} else {
						c.listener.OnConsumeFail(c.Ctx, c.Topic, msg, err)
						panic(err)
					}
				}
			}
		}(i)
	}
	// listen on system signal
	sigVal := <-s
	fmt.Printf("got signal:%v, quiting \n", sigVal)
}

type consumer interface {
	Fetch() (*Msg, error)
}

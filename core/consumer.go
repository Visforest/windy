package core

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
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
	}
}

func WithDecompressFunc(f DecompressFunc) ConsumerOption {
	return func(c *ConsumerCore) {
		c.decompress = f
	}
}

func WithCompressFunc(f CompressFunc) ConsumerOption {
	return func(c *ConsumerCore) {
		c.compress = f
	}
}

type ConsumerCore struct {
	Ctx                 context.Context // required
	Topic               string          // required
	Processors          int             // required
	ConsumeFunc         ConsumeFunc     // required
	listener            ConsumeListener // optional
	BatchProcessCnt     int             // optional
	BatchProcessTimeout time.Duration   // optional
	uniq                UniqFunc        // optional
	decompress          DecompressFunc  // optional
	compress            CompressFunc    // optional
}

// fetch msgs in one fetch cycle
func (c *ConsumerCore) fetchBatchMsgs(consumer consumer, chOut chan<- *Msg) {
	// set timeout for fetching process
	ctx, cancel := context.WithTimeout(c.Ctx, c.BatchProcessTimeout)
	defer cancel()

	finish := false
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("fetchBatchMsgs timed out")
				finish = true
				return
			}
		}
	}()

	chMsg := make(chan *Msg, 1024)

	// fetch messages in multi goroutines
	for i := 0; i < c.Processors; i++ {
		go func() {
			for {
				m, err := consumer.Fetch()
				if c.listener != nil {
					c.listener.PrepareConsume(c.Ctx, c.Topic, m, err)
				}
				if err != nil {
					// fail, skip
					continue
				}
				chMsg <- m
			}
		}()
	}
	fetched := 0
	for m := range chMsg {
		chOut <- m
		fetched++
		if fetched >= c.BatchProcessCnt || finish {
			// get enough msgs or fetch timed out, quit
			break
		}
	}
}

func (c *ConsumerCore) fetchMany(consumer consumer, chOut chan<- *Msg) {
	if c.uniq != nil || c.compress != nil {
		// loop batch fetch msgs in limited count or limited time
		for {
			c.fetchBatchMsgs(consumer, chOut)
		}
	} else {
		// fetch messages in multi goroutines
		for i := 0; i < c.Processors; i++ {
			go func() {
				for {
					m, err := consumer.Fetch()
					if c.listener != nil {
						c.listener.PrepareConsume(c.Ctx, c.Topic, m, err)
					}
					if err != nil {
						// fail, skip
						continue
					}
					chOut <- m
				}
			}()
		}
	}
}

// fetch msgs from chIn,deduplicate,and then sent to chOut
func (c *ConsumerCore) deduplicateMsg(chIn <-chan *Msg, chOut chan<- *Msg) {
	for {
		var msgs = make([]*Msg, 0, c.BatchProcessCnt)
		for m := range chIn {
			msgs = append(msgs, m)
			if len(msgs) == c.BatchProcessCnt {
				break
			}
		}

		seen := make(map[string]struct{})
		for _, m := range msgs {
			id := c.uniq(m)
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
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
	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	// fetch msgs
	chMsg := make(chan *Msg, 1024)
	go c.fetchMany(consumer, chMsg)

	// decompress -> deduplicate -> compress
	chDecompressed := make(chan *Msg, 1024)
	chDeduplicated := make(chan *Msg, 1024)
	chCompressed := make(chan *Msg, 1024)

	// decompress
	if c.decompress == nil {
		chDecompressed = chMsg
	} else {
		go c.decompressMsg(chMsg, chDecompressed)
	}

	// deduplicate
	if c.uniq == nil {
		chDeduplicated = chDecompressed
	} else {
		go c.deduplicateMsg(chDecompressed, chDeduplicated)
	}

	// compress
	if c.compress == nil {
		chCompressed = chDeduplicated
	} else {
		go c.compressMsg(chDeduplicated, chCompressed)
	}

	// consume msgs in multi goroutines
	for i := 0; i < c.Processors; i++ {
		go func() {
			for {
				msg := <-chCompressed
				if c.listener == nil {
					_ = c.ConsumeFunc(c.Ctx, c.Topic, msg)
				} else {
					c.listener.PrepareConsume(c.Ctx, c.Topic, msg, nil)
					if err := c.ConsumeFunc(c.Ctx, c.Topic, msg); err == nil {
						c.listener.OnConsumeSucceed(c.Ctx, c.Topic, msg)
					} else {
						c.listener.OnConsumeFail(c.Ctx, c.Topic, msg, err)
					}
				}
			}
		}()
	}
	// listen on system signal
	sigVal := <-s
	fmt.Printf("got signal:%v, quiting \n", sigVal)
}

type consumer interface {
	Fetch() (*Msg, error)
}

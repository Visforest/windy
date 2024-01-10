package core

import (
	"context"
)

type ProducerOption func(producer *ProducerCore)

func WithProducerContext(ctx context.Context) ProducerOption {
	return func(p *ProducerCore) {
		p.Ctx = ctx
	}
}

func WithIdCreator(creator IdCreator) ProducerOption {
	return func(p *ProducerCore) {
		p.IdCreator = creator
	}
}

func WithProducerListener(listener ProducerListener) ProducerOption {
	return func(p *ProducerCore) {
		p.listener = listener
	}
}

type ProducerCore struct {
	Ctx       context.Context
	Topic     string
	IdCreator IdCreator
	listener  ProducerListener
}

func (p *ProducerCore) Send(producer Producer, m *Msg) (string, error) {
	// generate msg id
	m.Id = p.IdCreator.Create()
	var err error

	if p.listener == nil {
		err = producer.Push(m)
	} else {
		// before send
		p.listener.PrepareSend(p.Ctx, p.Topic, m, nil)
		// send
		err = producer.Push(m)
		if err != nil {
			// on send fail
			p.listener.OnSendFail(p.Ctx, p.Topic, m, err)
			return "", err
		}
		// after send
		p.listener.OnSendSucceed(p.Ctx, p.Topic, m)
	}
	return m.Id, err
}

type Producer interface {
	Push(m *Msg) error
}

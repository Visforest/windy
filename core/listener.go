package core

import (
	"context"
)

type ProducerListener interface {
	// PrepareSend does something before msg is sent
	PrepareSend(ctx context.Context, topic string, msg *Msg, err error)

	// OnSendSucceed does something after msg is sent successfully
	OnSendSucceed(ctx context.Context, topic string, msg *Msg)

	// OnSendFail does something when msg is failed to be sent
	OnSendFail(ctx context.Context, topic string, msg *Msg, err error)
}

type ConsumeListener interface {
	// PrepareConsume does something before data is handled by your handler logic
	PrepareConsume(ctx context.Context, topic string, msg *Msg, err error)

	// OnConsumeSucceed does something after data is handled by your handler logic successfully
	OnConsumeSucceed(ctx context.Context, topic string, msg *Msg)

	// OnConsumeFail does something when data is failed to handled by your handler logic
	OnConsumeFail(ctx context.Context, topic string, msg *Msg, err error)
}

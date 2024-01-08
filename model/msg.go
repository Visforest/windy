package model

type Msg struct {
	Id   string
	Data any
}

func NewMsg(data any) *Msg {
	return &Msg{Data: data}
}

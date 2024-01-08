package model

type Msg struct {
	Id   string `json:"id"`
	Data any    `json:"data"`
}

func NewMsg(data any) *Msg {
	return &Msg{Data: data}
}

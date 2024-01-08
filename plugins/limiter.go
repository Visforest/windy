package plugins

type Limiter interface {
	BlockOK() bool
}

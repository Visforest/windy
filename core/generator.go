package core

import "github.com/bwmarrin/snowflake"

type IdCreator interface {
	Create() string
}

type SnowflakeCreator struct{ node int64 }

func NewSnowflakeCreator(node int64) *SnowflakeCreator {
	return &SnowflakeCreator{node: node}
}

func (s *SnowflakeCreator) Create() string {
	node, _ := snowflake.NewNode(s.node)
	return node.Generate().String()
}

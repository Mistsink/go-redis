package database

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

type EchoDB struct{}

func NewEchoDB() *EchoDB {
	return &EchoDB{}
}

func (e *EchoDB) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.NewMultiBulkReply(args)
}

func (e *EchoDB) Close() {
}

func (e *EchoDB) AfterClientClose(conn resp.Connection) {
}

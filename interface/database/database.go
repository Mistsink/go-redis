package database

import (
	"go-redis/interface/resp"
)

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args CmdLine) resp.Reply
	Close()
	AfterClientClose(conn resp.Connection)
}

type DataEntity struct {
	Data any
}

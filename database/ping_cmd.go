package database

import (
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func Ping(db *DB, args dbface.CmdLine) resp.Reply {
	return reply.NewPongReply()
}

func init() {
	RegisterCmd("PING", Ping, 1)
}

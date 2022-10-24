package database

import (
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"strings"
)

var cmdTable = make(map[string]*command)

type ExecFunc = func(db *DB, args dbface.CmdLine) resp.Reply

type command struct {
	executor ExecFunc
	arity    int //	参数数量 含命令名称
}

func RegisterCmd(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}

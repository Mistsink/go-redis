package database

import (
	"go-redis/datastruct/dict"
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

type DB struct {
	index int
	data  dict.Dict

	addAOF func(cmdLine dbface.CmdLine)
}

func makeDB() *DB {
	return &DB{data: dict.NewSyncDict(),
		addAOF: func(cmdLine dbface.CmdLine) {}}
}

func (db *DB) Exec(client resp.Connection, cmdLine dbface.CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]

	if !ok {
		return reply.NewStandardErrReply("Err unknown command " + cmdName)
	}

	if !validateArity(cmd.arity, cmdLine) {
		return reply.NewArgNumErrReply(cmdName)
	}

	return cmd.executor(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs dbface.CmdLine) bool {
	argsNum := len(cmdArgs)
	if arity > 0 {
		return argsNum == arity
	}

	return argsNum >= -argsNum
}

func (db *DB) GetEntity(key string) (*dbface.DataEntity, bool) {
	val, exists := db.data.Get(key)
	if !exists {
		return nil, false
	}
	return val.(*dbface.DataEntity), true
}

func (db *DB) PutEntity(key string, val *dbface.DataEntity) (n int) {
	return db.data.Put(key, val)
}
func (db *DB) PutIfExists(key string, val *dbface.DataEntity) (n int) {
	return db.data.PutIfExists(key, val)
}
func (db *DB) PutIfAbsent(key string, val *dbface.DataEntity) (n int) {
	return db.data.PutIfAbsent(key, val)
}
func (db *DB) Remove(key string) (n int) {
	return db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (n int) {
	for _, key := range keys {
		n += db.data.Remove(key)
	}
	return
}

func (db *DB) Flush() {
	db.data.Clear()
}

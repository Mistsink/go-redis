package database

import (
	"go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func init() {
	RegisterCmd("GET", execGet, 2)
	RegisterCmd("SET", execSet, 3)
	RegisterCmd("SetNX", execSetNX, 3)
	RegisterCmd("GETSET", execGetSet, 3)
	RegisterCmd("STRLEN", execStrLen, 2)

}

func execGet(db *DB, args database.CmdLine) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	return reply.NewBulkReply(entity.Data.([]byte))
}

// SET k v
func execSet(db *DB, args database.CmdLine) resp.Reply {
	key, val := string(args[0]), args[1]

	db.PutEntity(key, &database.DataEntity{Data: val})
	db.addAOF(utils.ToCmdLine2("get", args...))
	return reply.NewOkReply()
}

func execSetNX(db *DB, args database.CmdLine) resp.Reply {
	key, val := string(args[0]), args[1]

	n := db.PutIfAbsent(key, &database.DataEntity{Data: val})
	db.addAOF(utils.ToCmdLine2("setnx", args...))
	return reply.NewIntReply(int64(n))
}

// GETSET k v
// get old set new
func execGetSet(db *DB, args database.CmdLine) resp.Reply {
	key, val := string(args[0]), args[1]
	entity, ok := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{Data: val})
	db.addAOF(utils.ToCmdLine2("getset", args...))

	if !ok {
		return reply.NewNullBulkReply()
	}
	return reply.NewBulkReply(entity.Data.([]byte))
}

// STRLEN k
// the length of the val found by the key
func execStrLen(db *DB, args database.CmdLine) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	return reply.NewIntReply(int64(len(entity.Data.([]byte))))
}

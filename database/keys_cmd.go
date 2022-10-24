package database

import (
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

func init() {
	RegisterCmd("DEL", execDel, -2) //	arity >= 2
	RegisterCmd("EXISTS", execExists, -2)
	RegisterCmd("FLUSHDB", execFlushDB, -1)
	RegisterCmd("TYPE", execType, 2)
	RegisterCmd("RENAME", execRename, 3)
	RegisterCmd("RENAMENX", execRenameNX, 3)
	RegisterCmd("KEYS", execKeys, 2)
}

// execDel executor for 'DEL'
func execDel(db *DB, args dbface.CmdLine) resp.Reply {
	keys := bytes2Strs(args)

	n := db.Removes(keys...)
	if n > 0 {
		db.addAOF(utils.ToCmdLine2("del", args...))
	}
	return reply.NewIntReply(int64(n))
}

// execExists executor for 'Exists'
func execExists(db *DB, args dbface.CmdLine) resp.Reply {
	keys := bytes2Strs(args)

	n := int64(0)

	for _, key := range keys {
		_, ok := db.GetEntity(key)
		if ok {
			n++
		}
	}
	return reply.NewIntReply(n)
}

// execFlushDB executor for 'FlushDB'
func execFlushDB(db *DB, args dbface.CmdLine) resp.Reply {
	db.Flush()
	db.addAOF(utils.ToCmdLine2("flushdb", args...))
	return reply.NewOkReply()
}

// execType executor for 'Type'
func execType(db *DB, args dbface.CmdLine) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return reply.NewStatusReply("none")
	}

	switch entity.Data.(type) {
	// TODO: other type
	case []byte:
		return reply.NewStatusReply("string")
	}

	return reply.NewUnknownErrReply()
}

// execRename executor for 'Rename' 会强制覆盖 newKey
func execRename(db *DB, args dbface.CmdLine) resp.Reply {
	oldKey, newKey := string(args[0]), string(args[1])
	entity, ok := db.GetEntity(oldKey)
	if !ok {
		return reply.NewStandardErrReply("no such key")
	}

	db.PutEntity(newKey, entity)
	db.Remove(oldKey)
	db.addAOF(utils.ToCmdLine2("rename", args...))
	return reply.NewOkReply()
}

// execRenameNX executor for 'RenameNX'	只有当 newKey 不存在时才会更新
func execRenameNX(db *DB, args dbface.CmdLine) resp.Reply {
	oldKey, newKey := string(args[0]), string(args[1])

	_, ok := db.GetEntity(newKey)
	if ok {
		return reply.NewIntReply(0)
	}

	entity, ok := db.GetEntity(oldKey)
	if !ok {
		return reply.NewStandardErrReply("no such key")
	}

	db.PutEntity(newKey, entity)
	db.Remove(oldKey)
	db.addAOF(utils.ToCmdLine2("renamenx", args...))
	return reply.NewIntReply(1)
}

// execKeys executor for 'Keys'
func execKeys(db *DB, args dbface.CmdLine) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))

	keys := make([][]byte, 0)

	db.data.ForEach(func(key string, _ any) bool {
		if pattern.IsMatch(key) {
			keys = append(keys, []byte(key))
		}

		return true
	})

	return reply.NewMultiBulkReply(keys)
}

func bytes2Strs(tar [][]byte) []string {
	ret := make([]string, len(tar))
	for i, key := range tar {
		ret[i] = string(key)
	}
	return ret
}

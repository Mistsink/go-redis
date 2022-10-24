package database

import (
	"fmt"
	"go-redis/aof"
	"go-redis/config"
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AOFHandler
}

func NewStandaloneDataBase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)

	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}

	//	开启 AOF
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(database) //	初始化时会进行数据恢复
		if err != nil {
			panic(err)
		}

		database.aofHandler = aofHandler

		for _, db := range database.dbSet {
			_db := db //	另外声明局部变量，解决闭包问题
			_db.addAOF = func(cmdLine dbface.CmdLine) {
				database.aofHandler.AddAOF(_db.index, cmdLine)
			}
		}
	}

	return database
}

func (database *StandaloneDatabase) Exec(client resp.Connection, cmdLine dbface.CmdLine) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	switch strings.ToLower(string(cmdLine[0])) {
	case "select":
		if len(cmdLine) != 2 {
			return reply.NewArgNumErrReply("select")
		}
		return execSelectDB(client, database, cmdLine[1:])
	default:
		db := database.dbSet[client.GetDBIndex()]
		return db.Exec(client, cmdLine)
	}
}

func (database *StandaloneDatabase) Close() {
	fmt.Println("database close")
}

func (database *StandaloneDatabase) AfterClientClose(conn resp.Connection) {
	fmt.Println("database AfterClientClose")
}

func execSelectDB(conn resp.Connection, database *StandaloneDatabase, args dbface.CmdLine) resp.Reply {
	dbIdx, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.NewStandardErrReply("Err invalidate DB index")
	}
	if dbIdx >= len(database.dbSet) {
		return reply.NewStandardErrReply("Err DB index is out of range")
	}

	conn.SelectDB(dbIdx)
	return reply.NewOkReply()
}

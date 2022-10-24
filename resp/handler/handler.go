package handler

import (
	"context"
	"go-redis/cluster"
	"go-redis/config"
	"go-redis/database"
	dbface "go-redis/interface/database"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/resp/conn"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

const networkClosedStr = "use of closed network connection"

type RESPHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
	db         dbface.Database
}

func NewRESPHandler() *RESPHandler {
	var db dbface.Database

	//	select standalone or cluster with the config
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.NewClusterDatabase()
	} else {
		db = database.NewStandaloneDataBase()
	}

	return &RESPHandler{
		db: db,
	}
}

func (r *RESPHandler) closeClient(conn *conn.Conn) {
	_ = conn.Close()
	r.activeConn.Delete(conn)
	r.db.AfterClientClose(conn)
}

func (r *RESPHandler) Handle(ctx context.Context, c net.Conn) {
	if r.closing.Get() {
		_ = c.Close()
		return
	}

	client := conn.NewConn(c)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(c)

	for payload := range ch {
		//	error
		if payload.Err != nil {

			// error must close conn
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), networkClosedStr) {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			// protocol error
			errReply := reply.NewStandardErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			continue
		}

		//	exec
		if payload.Data == nil {
			continue
		}

		multiBulkReply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Info("require multi bulk reply")
			continue
		}

		execReply := r.db.Exec(client, multiBulkReply.Args)
		if execReply != nil {
			_ = client.Write(execReply.ToBytes())
		} else {
			_ = client.Write(reply.NewUnknownErrReply().ToBytes())
		}
	}
}

func (r *RESPHandler) Close() error {
	logger.Info("Handler shutting down")
	r.closing.Set(true)

	r.activeConn.Range(func(key, value any) bool {
		client := key.(*conn.Conn)
		_ = client.Close()
		return true
	})

	r.db.Close()
	return nil
}

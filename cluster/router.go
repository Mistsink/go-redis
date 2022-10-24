package cluster

import (
	"go-redis/interface/resp"
	"strings"
)

type CmdFunc func(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply

func newRouter() map[string]CmdFunc {
	router := make(map[string]CmdFunc)
	//	ops that influence one key
	opsWithOneKey := []string{
		"exists",
		"type",
		"set",
		"setnx",
		"get",
		"getset",
	}
	for _, op := range opsWithOneKey {
		op = strings.ToLower(op)
		router[op] = defaultRelayFunc
	}

	router["ping"] = cmdPing
	router["select"] = cmdSelect
	router["rename"] = cmdRename
	router["renamenx"] = cmdRename
	router["flushdb"] = cmdFlushDB
	router["del"] = cmdDel

	return router
}

func defaultRelayFunc(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	key := string(cmdLine[1])
	peerAddr := cluster.peerPicker.PickNode(key)
	return cluster.relay(peerAddr, conn, cmdLine)
}

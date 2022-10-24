package cluster

import (
	"context"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
	"strconv"
)

func (cluster *ClusterDatabase) borrowPeerClient(peerAddr string) (*client.Client, error) {
	pool, ok := cluster.peerConn[peerAddr]
	if !ok {
		return nil, errors.New("connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type of assertion")
	}

	return c, nil
}

func (cluster *ClusterDatabase) returnPeerClient(peerAddr string, peerClient *client.Client) error {
	pool, ok := cluster.peerConn[peerAddr]
	if !ok {
		return errors.New("connection not found")
	}
	return pool.ReturnObject(context.Background(), peerClient)
}

func (cluster *ClusterDatabase) relay(peerAddr string, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	if peerAddr == cluster.self {
		return cluster.db.Exec(conn, cmdLine)
	}

	peerClient, err := cluster.borrowPeerClient(peerAddr)
	if err != nil {
		return reply.NewStandardErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peerAddr, peerClient)
	}()
	//	首先选择正确的 DB
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(conn.GetDBIndex())))
	return peerClient.Send(cmdLine)
}

func (cluster *ClusterDatabase) broadcast(conn resp.Connection, cmdLine [][]byte) map[string]resp.Reply {
	relayReplies := make(map[string]resp.Reply)
	for _, nodeAddr := range cluster.nodes {
		relayReply := cluster.relay(nodeAddr, conn, cmdLine)
		relayReplies[nodeAddr] = relayReply
	}
	return relayReplies
}

package cluster

import (
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func cmdPing(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	return cluster.db.Exec(conn, cmdLine)
}
func cmdSelect(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	return cluster.db.Exec(conn, cmdLine)
}

func cmdRename(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	if len(cmdLine) != 3 {
		return reply.NewStandardErrReply("Err Wrong number args")
	}

	oldKey, newKey := string(cmdLine[1]), string(cmdLine[2])
	oldPeerAddr, newPeerAddr := cluster.peerPicker.PickNode(oldKey), cluster.peerPicker.PickNode(newKey)

	if oldPeerAddr != newPeerAddr {
		return reply.NewStandardErrReply("Err rename must within one peer")
	}

	return cluster.relay(oldPeerAddr, conn, cmdLine)
}

func cmdFlushDB(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	replies := cluster.broadcast(conn, cmdLine)

	for _, r := range replies {
		if reply.IsErrReply(r) {
			return reply.NewStandardErrReply("error: " + r.(reply.ErrorReply).Error())
		}
	}

	return reply.NewOkReply()
}

func cmdDel(cluster *ClusterDatabase, conn resp.Connection, cmdLine [][]byte) resp.Reply {
	replies := cluster.broadcast(conn, cmdLine)
	n := int64(0)

	for _, r := range replies {
		if reply.IsErrReply(r) {
			return reply.NewStandardErrReply("error: " + r.(reply.ErrorReply).Error())
		}

		intReply, ok := r.(*reply.IntReply)
		if !ok {
			return reply.NewStandardErrReply("error: type assertion that translate into IntReply")
		}
		n += intReply.Code
	}

	return reply.NewIntReply(n)
}

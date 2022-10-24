package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	"go-redis/database"
	dbface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/resp/reply"
)

type ClusterDatabase struct {
	self string

	nodes      []string //	all node addr (include self)
	peerPicker *consistenthash.NodeMap
	peerConn   map[string]*pool.ObjectPool
	db         dbface.Database
}

var router = newRouter()

func NewClusterDatabase() *ClusterDatabase {
	c := &ClusterDatabase{
		self:       config.Properties.Self,
		nodes:      make([]string, 0, len(config.Properties.Peers)+1),
		peerPicker: consistenthash.NewNodeMap(nil),
		peerConn:   make(map[string]*pool.ObjectPool),
		db:         database.NewStandaloneDataBase(),
	}
	//	nodes
	c.nodes = append(c.nodes, c.self)
	for _, peerAddr := range config.Properties.Peers {
		c.nodes = append(c.nodes, peerAddr)
	}

	//	peerConn
	ctx := context.Background()
	for _, peerAddr := range config.Properties.Peers {
		c.peerConn[peerAddr] = pool.NewObjectPoolWithDefaultConfig(
			ctx, newConnFactory(peerAddr))
	}

	return c
}

func (cluster *ClusterDatabase) Exec(client resp.Connection, cmdLine dbface.CmdLine) (r resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			r = reply.NewUnknownErrReply()
		}
	}()

	cmdName := string(cmdLine[0])
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.NewStandardErrReply("Err not suppoerted cmd: " + cmdName)
	}

	return cmdFunc(cluster, client, cmdLine)
}

func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

func (cluster *ClusterDatabase) AfterClientClose(conn resp.Connection) {
	cluster.db.AfterClientClose(conn)
}

package conn

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Conn struct {
	conn         net.Conn
	waitingReply wait.Wait
	mu           sync.Mutex
	selectedDB   int
}

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
	}
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) Close() error {
	c.waitingReply.WaitWithTimeout(time.Second * 5)
	_ = c.conn.Close()
	return nil
}

func (c *Conn) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}

	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(bytes)
	return err
}

func (c *Conn) GetDBIndex() int {
	return c.selectedDB
}

func (c *Conn) SelectDB(dbIdx int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.selectedDB = dbIdx
}

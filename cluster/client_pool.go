package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/resp/client"
)

type connFactory struct {
	PeerAddr string
}

func newConnFactory(peerAddr string) *connFactory {
	return &connFactory{PeerAddr: peerAddr}
}

func (connFac *connFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.NewClient(connFac.PeerAddr)
	if err != nil {
		return nil, err
	}
	c.Start()

	object := pool.NewPooledObject(c)
	return object, nil
}

func (connFac *connFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(client.Client)
	if !ok {
		return errors.New("type mismatch")
	}

	c.Close()
	return nil
}

func (connFac *connFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (connFac *connFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (connFac *connFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

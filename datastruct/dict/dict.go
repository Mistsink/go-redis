package dict

type Consumer func(key string, val any) bool

type Dict interface {
	Get(key string) (val any, exists bool)
	Len() int
	Put(key string, val any) (n int)
	PutIfAbsent(key string, val any) (n int)
	PutIfExists(key string, val any) (n int)
	Remove(key string) (n int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}

package dict

import (
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func NewSyncDict() *SyncDict {
	return &SyncDict{m: sync.Map{}}
}

func (d *SyncDict) Get(key string) (val any, exists bool) {
	val, exists = d.m.Load(key)
	return
}

func (d *SyncDict) Len() int {
	//	TODO: 添加一个新的字段单独表示长度，而不再去遍历
	length := 0
	d.m.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

func (d *SyncDict) Put(key string, val any) (n int) {
	_, existed := d.m.Load(key)
	d.m.Store(key, val)
	if existed {
		return 0
	}
	return 1
}

func (d *SyncDict) PutIfAbsent(key string, val any) (n int) {
	_, existed := d.m.Load(key)
	if existed {
		return 0
	}

	d.m.Store(key, val)
	return 1
}

func (d *SyncDict) PutIfExists(key string, val any) (n int) {
	_, existed := d.m.Load(key)
	if existed {
		d.m.Store(key, val)
		return 1
	}
	return 0
}

func (d *SyncDict) Remove(key string) (n int) {
	_, existed := d.m.Load(key)
	if existed {
		d.m.Delete(key)
		return 1
	}
	return 0
}

func (d *SyncDict) ForEach(consumer Consumer) {
	d.m.Range(func(key, value any) bool {
		consumer(key.(string), value)
		return true
	})
}

func (d *SyncDict) Keys() []string {
	ret := make([]string, d.Len())
	cnt := 0
	d.m.Range(func(key, _ any) bool {
		ret[cnt] = key.(string)
		cnt++
		return true
	})
	return ret
}

// RandomKeys 	maybe duplicated
func (d *SyncDict) RandomKeys(limit int) []string {
	ret := make([]string, limit)
	for i := 0; i < limit; i++ {
		d.m.Range(func(key, _ any) bool {
			ret[i] = key.(string)
			return false
		})
	}

	return ret
}

func (d *SyncDict) RandomDistinctKeys(limit int) []string {
	if limit <= 0 {
		return []string{}
	}

	ret := make([]string, limit)
	cnt := 0

	d.m.Range(func(key, _ any) bool {
		ret[cnt] = key.(string)
		cnt++
		if cnt < limit {
			return true
		}
		return false
	})

	return ret
}

func (d *SyncDict) Clear() {
	*d = *NewSyncDict()
}

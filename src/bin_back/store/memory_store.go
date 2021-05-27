// Package store provides a simple in-memory key value store.
package store

import (
	"container/list"
	"log"
	"math"
)

var Logging bool

type strList []string

// In-memory storage implementation. All calls always returns nil.
type MemoryStorage struct {
	clock uint64

	strs  map[string]string
	lists map[string]*list.List
}

var _ Storage = new(MemoryStorage)

func NewMemoryStorageId(id int) *MemoryStorage {
	return &MemoryStorage{
		strs:  make(map[string]string),
		lists: make(map[string]*list.List),
	}
}

func NewMemoryStorage() *MemoryStorage {
	return NewMemoryStorageId(0)
}

func (self *MemoryStorage) Clock(atLeast uint64, ret *uint64) error {
	if self.clock < atLeast {
		self.clock = atLeast
	}

	*ret = self.clock

	if self.clock < math.MaxUint64 {
		self.clock++
	}

	if Logging {
		log.Printf("Clock(%d) => %d", atLeast, *ret)
	}

	return nil
}

func (self *MemoryStorage) Get(key string, value *string) error {
	*value = self.strs[key]

	if Logging {
		log.Printf("Get(%q) => %q", key, *value)
	}

	return nil
}

func (self *MemoryStorage) Set(kv *KeyValue, succ *bool) error {
	if kv.Value != "" {
		self.strs[kv.Key] = kv.Value
	} else {
		delete(self.strs, kv.Key)
	}

	*succ = true

	if Logging {
		log.Printf("Set(%q, %q)", kv.Key, kv.Value)
	}

	return nil
}

func (self *MemoryStorage) Keys(p *Pattern, r *List) error {
	ret := make([]string, 0, len(self.strs))

	for k := range self.strs {
		if p.Match(k) {
			ret = append(ret, k)
		}
	}

	r.L = ret

	if Logging {
		log.Printf("Keys(%q, %q) => %d", p.Prefix, p.Suffix, len(r.L))
		for i, s := range r.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *MemoryStorage) ListKeys(p *Pattern, r *List) error {
	ret := make([]string, 0, len(self.lists))
	for k := range self.lists {
		if p.Match(k) {
			ret = append(ret, k)
		}
	}

	r.L = ret

	if Logging {
		log.Printf("ListKeys(%q, %q) => %d", p.Prefix, p.Suffix, len(r.L))
		for i, s := range r.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *MemoryStorage) ListGet(key string, ret *List) error {
	if lst, found := self.lists[key]; !found {
		ret.L = []string{}
	} else {
		ret.L = make([]string, 0, lst.Len())
		for i := lst.Front(); i != nil; i = i.Next() {
			ret.L = append(ret.L, i.Value.(string))
		}
	}

	if Logging {
		log.Printf("ListGet(%q) => %d", key, len(ret.L))
		for i, s := range ret.L {
			log.Printf("  %d: %q", i, s)
		}
	}

	return nil
}

func (self *MemoryStorage) ListAppend(kv *KeyValue, succ *bool) error {
	lst, found := self.lists[kv.Key]
	if !found {
		lst = list.New()
		self.lists[kv.Key] = lst
	}

	lst.PushBack(kv.Value)

	*succ = true

	if Logging {
		log.Printf("ListAppend(%q, %q)", kv.Key, kv.Value)
	}

	return nil
}

func (self *MemoryStorage) ListRemove(kv *KeyValue, n *int) error {
	*n = 0

	lst, found := self.lists[kv.Key]
	if !found {
		return nil
	}

	i := lst.Front()
	for i != nil {
		if i.Value.(string) == kv.Value {
			hold := i
			i = i.Next()
			lst.Remove(hold)
			*n++
			continue
		}

		i = i.Next()
	}

	if lst.Len() == 0 {
		delete(self.lists, kv.Key)
	}

	if Logging {
		log.Printf("ListRemove(%q, %q) => %d", kv.Key, kv.Value, *n)
	}

	return nil
}

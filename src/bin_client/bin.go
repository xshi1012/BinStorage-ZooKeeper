package bin_client

import (
	"BinStorageZK/src/bin_back/store"
)

type Bin struct {
}

func (self *Bin) Clock(atLeast uint64, ret *uint64) error {
	panic("todo")
}

func (self *Bin) Get(key string, value *string) error {
	panic("todo")
}

func (self *Bin) Set(kv *store.KeyValue, succ *bool) error {
	panic("todo")
}

func (self *Bin) Keys(p *store.Pattern, list *store.List) error {
	panic("todo")
}

func (self *Bin) ListGet(key string, list *store.List) error {
	panic("todo")
}

func (self *Bin) ListAppend(kv *store.KeyValue, succ *bool) error {
	panic("todo")
}

func (self *Bin) ListRemove(kv *store.KeyValue, n *int) error {
	panic("todo")
}

func (self *Bin) ListKeys(p *store.Pattern, list *store.List) error {
	panic("todo")
}
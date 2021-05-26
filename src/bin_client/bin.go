package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/utils/colon"
)

type bin struct {
	name string
	key string
	binClient *binClient
}

func NewBin(name string, binClient *binClient) *bin {
	bin := new(bin)
	bin.name = name
	bin.key = colon.Escape(name) + bin_config.Delimiter
	bin.binClient = binClient

	return bin
}

func (self *bin) Clock(atLeast uint64, ret *uint64) error {
	binSingle, e := self.binClient.getBinSingleForBin(self.name)
	if e != nil {
		return e
	}

	return binSingle.clock(ret)
}

func (self *bin) Get(key string, value *string) error {
	panic("todo")
}

func (self *bin) Set(kv *store.KeyValue, succ *bool) error {
	panic("todo")
}

func (self *bin) Keys(p *store.Pattern, list *store.List) error {
	panic("todo")
}

func (self *bin) ListGet(key string, list *store.List) error {
	panic("todo")
}

func (self *bin) ListAppend(kv *store.KeyValue, succ *bool) error {
	panic("todo")
}

func (self *bin) ListRemove(kv *store.KeyValue, n *int) error {
	panic("todo")
}

func (self *bin) ListKeys(p *store.Pattern, list *store.List) error {
	panic("todo")
}

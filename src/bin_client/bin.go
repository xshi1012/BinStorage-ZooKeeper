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

	return binSingle.Clock(ret)
}

func (self *bin) Get(key string, value *string) error {
	realKey := bin_config.KeyValueLog + bin_config.Delimiter + self.key + colon.Escape(key)

	binSingle, e := self.binClient.getBinSingleForBin(self.name)
	if e != nil {
		return e
	}

	e = binSingle.Get(realKey, value)
	if e != nil {
		// retry once
		binSingle, e = self.binClient.getBinSingleForBin(self.name)
		if e != nil {
			return e
		}
		return binSingle.Get(realKey, value)
	} else {
		return nil
	}
}

func (self *bin) Set(kv *store.KeyValue, succ *bool) error {
	realKey := bin_config.KeyValueLog + bin_config.Delimiter + self.key + colon.Escape(kv.Key)

	binSingle, e := self.binClient.getBinSingleForBin(self.name)
	if e != nil {
		return e
	}

	e = binSingle.Set(store.KV(realKey, kv.Value), succ)
	if e != nil {
		binSingle, e = self.binClient.getBinSingleForBin(self.name)
		if e != nil {
			return e
		}
		return binSingle.Set(store.KV(realKey, kv.Value), succ)
	} else {
		return nil
	}
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

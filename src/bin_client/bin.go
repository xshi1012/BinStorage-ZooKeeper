package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/utils/colon"
	"strings"
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
	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	return single.Clock(atLeast, ret)
}

func (self *bin) Get(key string, value *string) error {
	realKey := bin_config.KeyValueLog + bin_config.Delimiter + self.key + colon.Escape(key)

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	e = single.Get(realKey, value)
	if e != nil {
		// retry once
		single, e = self.binClient.getBinSingleForBin(self.name, true)
		if e != nil {
			return e
		}
		return single.Get(realKey, value)
	} else {
		return nil
	}
}

func (self *bin) Set(kv *store.KeyValue, succ *bool) error {
	realKey := bin_config.KeyValueLog + bin_config.Delimiter + self.key + colon.Escape(kv.Key)

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	e = single.Set(store.KV(realKey, kv.Value), succ)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, false)
		if e != nil {
			return e
		}
		return single.Set(store.KV(realKey, kv.Value), succ)
	} else {
		return nil
	}
}

func (self *bin) Keys(p *store.Pattern, list *store.List) error {
	realPattern := store.Pattern{Prefix: bin_config.KeyValueLog + bin_config.Delimiter + self.key + colon.Escape(p.Prefix), Suffix: p.Suffix}

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	unescaped := store.List{L: nil}
	e = single.Keys(&realPattern, &unescaped)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, true)
		if e != nil {
			return e
		}
		e = single.Keys(&realPattern, &unescaped)
		if e != nil {
			return e
		}
	}

	list.L = make([]string, 0, len(unescaped.L))
	for _, v := range unescaped.L {
		list.L = append(list.L, colon.Unescape(strings.Split(v, bin_config.Delimiter)[2]))
	}

	return nil
}

func (self *bin) ListGet(key string, list *store.List) error {
	realKey := bin_config.ListLog + bin_config.Delimiter + self.key + colon.Escape(key)

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	e = single.ListGet(realKey, list)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, true)
		if e != nil {
			return e
		}
		return single.ListGet(realKey, list)
	} else {
		return nil
	}
}

func (self *bin) ListAppend(kv *store.KeyValue, succ *bool) error {
	realKey := bin_config.ListLog + bin_config.Delimiter + self.key + colon.Escape(kv.Key)

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	e = single.ListAppend(store.KV(realKey, kv.Value), succ)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, false)
		if e != nil {
			return e
		}
		return single.ListAppend(store.KV(realKey, kv.Value), succ)
	} else {
		return nil
	}
}

func (self *bin) ListRemove(kv *store.KeyValue, n *int) error {
	realKey := bin_config.ListLog + bin_config.Delimiter + self.key + colon.Escape(kv.Key)

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	e = single.ListRemove(store.KV(realKey, kv.Value), n)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, false)
		if e != nil {
			return e
		}
		return single.ListRemove(store.KV(realKey, kv.Value), n)
	} else {
		return nil
	}
}

func (self *bin) ListKeys(p *store.Pattern, list *store.List) error {
	realPattern := store.Pattern{Prefix: bin_config.ListLog + bin_config.Delimiter + self.key + colon.Escape(p.Prefix), Suffix: p.Suffix}

	single, e := self.binClient.getBinSingleForBin(self.name, false)
	if e != nil {
		return e
	}

	unescaped := store.List{L: nil}
	e = single.ListKeys(&realPattern, &unescaped)
	if e != nil {
		single, e = self.binClient.getBinSingleForBin(self.name, true)
		if e != nil {
			return e
		}
		e = single.ListKeys(&realPattern, &unescaped)
		if e != nil {
			return e
		}
	}

	list.L = make([]string, 0, len(unescaped.L))
	for _, v := range unescaped.L {
		list.L = append(list.L, colon.Unescape(strings.Split(v, bin_config.Delimiter)[2]))
	}

	return nil
}

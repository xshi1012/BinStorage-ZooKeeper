package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"net/rpc"
)

type binSingle struct {
	addr string
	conn *rpc.Client
}

func NewBinSingle(addr string) *binSingle {
	binSingle := new(binSingle)
	binSingle.addr = addr
	binSingle.conn = nil

	return binSingle
}

func (self *binSingle) tryConnect() error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	self.conn = conn

	return nil
}

func (self *binSingle) Clock(ret *uint64) error {
	*ret = 0

	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationClock, uint64(0), ret)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationClock, uint64(0), ret)
	}
	return e
}

func (self *binSingle) Get(key string, value *string) error {
	*value = ""

	// try connect if not connected
	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	// perform the call
	e := self.conn.Call(bin_config.OperationGet, key, value)

	// retry once on shutdown error
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationGet, key, value)
	}
	return e
}

func (self *binSingle) Set(kv *store.KeyValue, succ *bool) error {
	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationSet, kv, succ)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationSet, kv, succ)
	}
	return e
}

func (self *binSingle) Keys(pattern *store.Pattern, list *store.List) error {
	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationKeys, pattern, list)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationKeys, pattern, list)
	}
	return e
}

func (self *binSingle) ListGet(key string, list *store.List) error {
	list.L = make([]string, 0)

	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationListGet, key, list)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationListGet, key, list)
	}
	return e
}

func (self *binSingle) ListAppend(kv *store.KeyValue, succ *bool) error {
	*succ = false

	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationListAppend, kv, succ)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationListAppend, kv, succ)
	}
	return e
}

func (self *binSingle) ListRemove(kv *store.KeyValue, n *int) error {
	*n = 0

	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.OperationListRemove, kv, n)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.OperationListRemove, kv, n)
	}
	return e
}

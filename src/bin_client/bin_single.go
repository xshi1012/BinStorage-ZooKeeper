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

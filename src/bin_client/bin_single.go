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

func (self *binSingle) Clock(atLeast uint64, ret *uint64) error {
	*ret = 0
	return self.callOperation(bin_config.OperationClock, atLeast, ret)
}

func (self *binSingle) Get(key string, value *string) error {
	*value = ""
	return self.callOperation(bin_config.OperationGet, key, value)
}

func (self *binSingle) Set(kv *store.KeyValue, succ *bool) error {
	return self.callOperation(bin_config.OperationSet, kv, succ)
}

func (self *binSingle) Keys(pattern *store.Pattern, list *store.List) error {
	return self.callOperation(bin_config.OperationKeys, pattern, list)
}

func (self *binSingle) ListGet(key string, list *store.List) error {
	list.L = make([]string, 0)
	return self.callOperation(bin_config.OperationListGet, key, list)
}

func (self *binSingle) ListAppend(kv *store.KeyValue, succ *bool) error {
	*succ = false
	return self.callOperation(bin_config.OperationListAppend, kv, succ)
}

func (self *binSingle) ListRemove(kv *store.KeyValue, n *int) error {
	*n = 0
	return self.callOperation(bin_config.OperationListRemove, kv, n)
}

func (self *binSingle) ListKeys(pattern *store.Pattern, list *store.List) error {
	return self.callOperation(bin_config.OperationListKeys, pattern, list)
}

func (self *binSingle) callOperation(operation string, input interface{}, output interface{}) error {
	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(operation, input, output)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(operation, input, output)
	}
	return e
}

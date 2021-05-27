package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"net/rpc"
)

type backClient struct {
	addr string
	conn *rpc.Client
}

func NewBackClient(addr string) *backClient {
	binSingle := new(backClient)
	binSingle.addr = addr
	binSingle.conn = nil

	return binSingle
}

func (self *backClient) tryConnect() error {
	conn, e := rpc.DialHTTP("tcp", self.addr)
	if e != nil {
		return e
	}

	self.conn = conn

	return nil
}

func (self *backClient) Get(key string, value *string) error {
	*value = ""
	return self.callOperation(bin_config.OperationGet, key, value)
}

func (self *backClient) Keys(pattern *store.Pattern, list *store.List) error {
	return self.callOperation(bin_config.OperationKeys, pattern, list)
}

func (self *backClient) ListGet(key string, list *store.List) error {
	list.L = make([]string, 0)
	return self.callOperation(bin_config.OperationListGet, key, list)
}

func (self *backClient) ListKeys(pattern *store.Pattern, list *store.List) error {
	return self.callOperation(bin_config.OperationListKeys, pattern, list)
}

func (self *backClient) forwardLog(log *Log, succ *bool) error {
	return self.callOperation(bin_config.BackOperationForward, log, succ)
}

func (self *backClient) GetPrimaryData(request *ServerDataRequest, data *ServerData) error {
	data.D = make(map[string][]string)
	return self.callOperation(bin_config.BackOperationGetPrimaryData, request, data)
}

func (self *backClient) callOperation(operation string, input interface{}, output interface{}) error {
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

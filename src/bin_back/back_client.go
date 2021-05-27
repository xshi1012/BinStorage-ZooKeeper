package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
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

func (self *backClient) forwardLog(log *Log, succ *bool) error {
	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.BackOperationForward, log, succ)

	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.BackOperationForward, log, succ)
	}

	return e
}

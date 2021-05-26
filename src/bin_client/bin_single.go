package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
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

func (self *binSingle) clock(ret *uint64) error {
	*ret = 0

	if self.conn == nil {
		e := self.tryConnect()
		if e != nil {
			return e
		}
	}

	e := self.conn.Call(bin_config.ServiceName + ".Clock", uint64(0), ret)
	if e == rpc.ErrShutdown {
		e = self.tryConnect()
		if e != nil {
			return e
		}

		e = self.conn.Call(bin_config.ServiceName + ".Clock", uint64(0), ret)
	}

	return e
}

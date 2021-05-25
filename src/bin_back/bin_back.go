package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/synchronization"
	"net"
	"net/http"
	"net/rpc"
	"github.com/go-zookeeper/zk"
	"time"
)

const (
	GroupPath = "back"
)

type BinBack struct {
	config *bin_config.BackConfig
}

func NewBinBack(b *bin_config.BackConfig) *BinBack {
	binBack := new(BinBack)
	binBack.config = b
	return binBack
}

func (self *BinBack) Run() error {
	server := rpc.NewServer()
	server.RegisterName("Storage", self.config.Store)

	l, e := net.Listen("tcp", self.config.Addr)

	if e != nil {
		if self.config.Ready != nil {
			self.config.Ready <- false
		}
		return e
	}

	conn, _, e := zk.Connect(self.config.Keepers, time.Second)
	if e != nil {
		if self.config.Ready != nil {
			self.config.Ready <- false
		}
		return e
	}

	if self.config.Ready != nil {
		self.config.Ready <- true
	}

	groupMember := synchronization.NewGroupMember(conn, GroupPath, self.config.Addr)
	_, e = groupMember.Join()
	go groupMember.Listen(func([]string){
		
	})

	e = http.Serve(l, server)

	if e != nil {
		if self.config.Ready != nil {
			self.config.Ready <- false
		}
		return e
	}


	return e
}
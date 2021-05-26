package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/synchronization"
	"fmt"
	"github.com/go-zookeeper/zk"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type binBack struct {
	config *bin_config.BackConfig
	zkConn *zk.Conn
	group *synchronization.GroupMember
	currentMembers []string
	zkClock *synchronization.DistriButedAtomicUint64
}

func NewBinBack(b *bin_config.BackConfig) *binBack {
	binBack := new(binBack)
	binBack.config = b

	return binBack
}

/* RPC handlers */
func (self *binBack) Clock(_ uint64, ret *uint64) error {
	clock, e := self.zkClock.GetAndIncrement()
	*ret = clock
	return e
}

/* server logics */
func (self *binBack) Run() error {
	server := rpc.NewServer()
	server.RegisterName(bin_config.ServiceName, self)

	l, e := net.Listen("tcp", self.config.Addr)
	if e != nil {
		self.failOnStart()
		return e
	}

	// connect to zookeeper
	conn, _, e := zk.Connect(self.config.Keepers, time.Second)
	if e != nil {
		self.failOnStart()
		return e
	}
	self.zkConn = conn

	// init clock
	self.zkClock = synchronization.NewDistributedAtomicLong(self.zkConn, bin_config.ServiceClockName, bin_config.ServiceClocksPath)
	e = self.zkClock.Init()
	if e != nil {
		self.failOnStart()
		return e
	}

	// do stuff before joining the group
	self.group = synchronization.NewGroupMember(conn, bin_config.GroupPath, self.config.Addr)
	e = self.preJoin()
	if e != nil {
		self.failOnStart()
		return e
	}

	// join the backend servers
	_, e = self.group.Join()
	if e != nil {
		self.failOnStart()
		return e
	}
	go self.group.Listen(self.handleGroupMemberChange)
	
	if self.config.Ready != nil {
		self.config.Ready <- true
	}

	e = http.Serve(l, server)

	return e
}

func (self *binBack) preJoin() error {
	members, e := self.group.GetCurrentMembers()
	self.currentMembers = members

	return e
}

func (self *binBack) handleGroupMemberChange(members []string) {
	fmt.Println(members)
}

func (self *binBack) failOnStart() {
	if self.config.Ready != nil {
		self.config.Ready <- false
	}
}

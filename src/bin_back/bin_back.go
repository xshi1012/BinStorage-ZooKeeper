package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/synchronization"
	"BinStorageZK/src/utils"
	"fmt"
	"github.com/go-zookeeper/zk"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type binBack struct {
	backClients    map[string]*backClient
	memberLock     sync.Mutex
	keyLocks       map[string]*sync.Mutex
	lockMapLock    sync.Mutex
	config         *bin_config.BackConfig
	zkConn         *zk.Conn
	group          *synchronization.GroupMember
	currentMembers []string
	zkClock        *synchronization.DistriButedAtomicUint64
}

func NewBinBack(b *bin_config.BackConfig) *binBack {
	binBack := new(binBack)
	binBack.config = b
	binBack.backClients = make(map[string]*backClient)
	binBack.keyLocks = make(map[string]*sync.Mutex)

	return binBack
}

/* RPC handlers */
// client RPCs
func (self *binBack) Clock(_ uint64, ret *uint64) error {
	clock, e := self.zkClock.GetAndIncrement()
	*ret = clock
	return e
}

func (self *binBack) Get(key string, value *string) error {
	lock := self.getLockForKey(key)
	lock.Lock()
	defer lock.Unlock()

	l := store.List{L: nil}
	_ = self.config.Store.ListGet(key, &l)

	logs, _ := ParseLog(l.L)
	log := ReplayKeyValueLog(logs)

	*value = log.Value

	return nil
}

func (self *binBack) Set(kv *store.KeyValue, succ *bool) error {
	lock := self.getLockForKey(kv.Key)
	lock.Lock()
	defer lock.Unlock()

	clock, e := self.zkClock.GetAndIncrement()
	if e != nil {
		return e
	}
	log := CreateLog(kv.Key, "", kv.Value, clock)

	_ = self.sendLogToReplica(log)

	logString, _ := utils.ObjectToString(log)
	return self.config.Store.ListAppend(store.KV(kv.Key, logString), succ)
}

func (self *binBack) Keys(pattern *store.Pattern, list *store.List) error {
	return self.config.Store.ListKeys(pattern, list)
}

// server internal RPCs
func (self *binBack) ForwardLog(log *Log, succ *bool) error {
	key := log.Key

	lock := self.getLockForKey(key)
	lock.Lock()

	go func() {
		logString, _ := utils.ObjectToString(log)
		var succ bool
		_ = self.config.Store.ListAppend(store.KV(key, logString), &succ)
		lock.Unlock()
	}()

	*succ = true
	return nil
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
	members, e := self.group.GetCurrentMembers()

	self.memberLock.Lock()
	self.currentMembers = members
	self.memberLock.Unlock()

	if e != nil {
		self.failOnStart()
		return e
	}

	// join the backend servers
	go self.group.Listen(self.handleGroupMemberChange)
	_, e = self.group.Join()
	if e != nil {
		self.failOnStart()
		return e
	}
	
	if self.config.Ready != nil {
		self.config.Ready <- true
	}

	e = http.Serve(l, server)

	return e
}

func (self *binBack) handleGroupMemberChange(members []string) {
	self.memberLock.Lock()
	self.currentMembers = members
	self.memberLock.Unlock()

	fmt.Println(members)
}

func (self *binBack) failOnStart() {
	if self.config.Ready != nil {
		self.config.Ready <- false
	}
}

func (self *binBack) sendLogToReplica(log *Log) error {
	// find the replicas
	var waitForMember string

	self.memberLock.Lock()
	i := 0
	for idx, v := range self.currentMembers {
		if v == self.config.Addr {
			i = idx + 1
			break
		}
	}
	fmt.Println(self.currentMembers)
	waitForMember = self.currentMembers[i % len(self.currentMembers)]
	self.memberLock.Unlock()

	if waitForMember == self.config.Addr {
		return nil
	}

	client, ok := self.backClients[waitForMember]
	if !ok {
		client = NewBackClient(waitForMember)
		self.backClients[waitForMember] = client
	}

	var succ bool
	return client.forwardLog(log, &succ)
}

func (self *binBack) getLockForKey(name string) *sync.Mutex {
	self.lockMapLock.Lock()
	defer self.lockMapLock.Unlock()

	lock, ok := self.keyLocks[name]
	if ok {
		return lock
	}

	lock = new(sync.Mutex)
	self.keyLocks[name] = lock

	return lock
}

package bin_back

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/synchronization"
	"BinStorageZK/src/utils"
	"BinStorageZK/src/utils/node_ring"
	"github.com/go-zookeeper/zk"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
)

type binBack struct {
	ready		   bool
	backClients    map[string]*backClient
	memberLock     sync.Mutex
	keyLocks       map[string]*sync.Mutex
	lockMapLock    sync.Mutex
	config         *bin_config.BackConfig
	zkConn         *zk.Conn
	group          *synchronization.GroupMember
	currentMembers []string
	zkClock        *synchronization.DistriButedAtomicUint64
	ring 		   *node_ring.NodeRing
}

func NewBinBack(b *bin_config.BackConfig) *binBack {
	bBack := new(binBack)
	bBack.config = b
	bBack.backClients = make(map[string]*backClient)
	bBack.keyLocks = make(map[string]*sync.Mutex)
	bBack.ready = false
	bBack.ring = node_ring.NewNodeRing(b.Backs, make([]string, 0))

	return bBack
}

/* RPC handlers */
// client RPCs
func (self *binBack) Clock(_ uint64, ret *uint64) error {
	clock, e := self.zkClock.GetAndIncrement()
	*ret = clock
	return e
}

func (self *binBack) Get(key string, value *string) error {
	if !self.ready {
		bin := strings.Split(key, bin_config.Delimiter)[1]
		return self.forwardReadRequest(bin, bin_config.OperationGet, key, value)
	}

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
	if !self.ready {
		bin := strings.Split(pattern.Prefix, bin_config.Delimiter)[1]
		return self.forwardReadRequest(bin, bin_config.OperationKeys, pattern, list)
	}

	keys := store.List{L: nil}
	_ = self.config.Store.ListKeys(pattern, &keys)

	exists := make([]string, 0)

	for _, v := range keys.L {
		lock := self.getLockForKey(v)
		lock.Lock()

		l := store.List{L: nil}
		_ = self.config.Store.ListGet(v, &l)

		logs, _ := ParseLog(l.L)
		log := ReplayKeyValueLog(logs)

		if log.Value != "" {
			exists = append(exists, v)
		}

		lock.Unlock()
	}

	list.L = exists
	return nil
}

func (self *binBack) ListGet(key string, list *store.List) error {
	if !self.ready {
		bin := strings.Split(key, bin_config.Delimiter)[1]
		return self.forwardReadRequest(bin, bin_config.OperationListGet, key, list)
	}

	lock := self.getLockForKey(key)
	lock.Lock()
	defer lock.Unlock()

	l := store.List{L: nil}
	_ = self.config.Store.ListGet(key, &l)

	logs, _ := ParseListLog(l.L)
	list.L = ReplayListLog(logs)

	return nil
}

func (self *binBack) ListAppend(kv *store.KeyValue, succ *bool) error {
	lock := self.getLockForKey(kv.Key)
	lock.Lock()
	defer lock.Unlock()

	clock, e := self.zkClock.GetAndIncrement()
	if e != nil {
		return e
	}
	log := CreateLog(kv.Key, bin_config.ListLogAppend, kv.Value, clock)

	_ = self.sendLogToReplica(log)

	logString, _ := utils.ObjectToString(log)
	return self.config.Store.ListAppend(store.KV(kv.Key, logString), succ)
}

func (self *binBack) ListRemove(kv *store.KeyValue, n *int) error {
	lock := self.getLockForKey(kv.Key)
	lock.Lock()
	defer lock.Unlock()

	clock, e := self.zkClock.GetAndIncrement()
	if e != nil {
		return e
	}
	log := CreateLog(kv.Key, bin_config.ListLogDelete, kv.Value, clock)
	logString, _ := utils.ObjectToString(log)

	_ = self.sendLogToReplica(log)

	succ := false
	e = self.config.Store.ListAppend(store.KV(kv.Key, logString), &succ)
	if e != nil {
		return e
	}
	*n = self.applyListDelete(log)

	return nil
}

func (self *binBack) ListKeys(pattern *store.Pattern, list *store.List) error {
	if !self.ready {
		bin := strings.Split(pattern.Prefix, bin_config.Delimiter)[1]
		return self.forwardReadRequest(bin, bin_config.OperationListKeys, pattern, list)
	}

	keys := store.List{L: nil}
	_ = self.config.Store.ListKeys(pattern, &keys)

	exists := make([]string, 0)

	for _, v := range keys.L {
		lock := self.getLockForKey(v)
		lock.Lock()

		l := store.List{L: nil}
		_ = self.config.Store.ListGet(v, &l)

		logs, _ := ParseListLog(l.L)
		ll := ReplayListLog(logs)

		if len(ll) > 0 {
			exists = append(exists, v)
		}

		lock.Unlock()
	}

	list.L = exists
	return nil
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
		if log.Operation == bin_config.ListLogDelete {
			self.applyListDelete(log)
		}
		lock.Unlock()
	}()

	*succ = true
	return nil
}

func (self *binBack) GetPrimaryData(request *ServerDataRequest, data *ServerData) error {
	target := request.Addr
	members := request.Members
	data.D = make(map[string][]string)

	keys := store.List{L: nil}
	_ = self.config.Store.ListKeys(&store.Pattern{Prefix: bin_config.KeyValueLog}, &keys)

	for _, v := range keys.L {
		bin := strings.Split(v, bin_config.Delimiter)[1]
		if !self.determineIfPrimary(target, bin, members) {
			continue
		}

		lock := self.getLockForKey(v)
		lock.Lock()

		l := store.List{L: nil}
		_ = self.config.Store.ListGet(v, &l)

		logs, _ := ParseLog(l.L)
		log := ReplayKeyValueLog(logs)

		if log.Value != "" {
			data.D[v] = l.L
		}

		if request.Delete {
			for _, s := range l.L {
				n := 0
				_ = self.config.Store.ListRemove(store.KV(v, s), &n)
			}
		}

		lock.Unlock()
	}

	keys = store.List{L: nil}
	_ = self.config.Store.ListKeys(&store.Pattern{Prefix: bin_config.ListLog}, &keys)

	for _, v := range keys.L {
		bin := strings.Split(v, bin_config.Delimiter)[1]
		if !self.determineIfPrimary(target, bin, members) {
			continue
		}

		lock := self.getLockForKey(v)
		lock.Lock()

		l := store.List{L: nil}
		_ = self.config.Store.ListGet(v, &l)

		logs, _ := ParseListLog(l.L)
		ll := ReplayListLog(logs)

		if len(ll) > 0 {
			data.D[v] = l.L
		}

		if request.Delete {
			for _, s := range l.L {
				n := 0
				_ = self.config.Store.ListRemove(store.KV(v, s), &n)
			}
		}

		lock.Unlock()
	}

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
	if e != nil {
		self.failOnStart()
		return e
	}

	sort.Strings(members)

	self.memberLock.Lock()
	self.currentMembers = members
	for _, v := range members {
		self.ring.NodeJoin(v)
	}
	self.memberLock.Unlock()

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
	sort.Strings(members)

	self.memberLock.Lock()
	if len(self.currentMembers) == 0 {
		self.ring.NodeJoin(self.config.Addr)
		self.ready = true
	} else if len(members) < len(self.currentMembers) {
		// node leaves
		for _, v := range self.currentMembers {
			if utils.IndexOf(members, v) < 0 {
				self.handleNodeLeave(self.currentMembers, v)
				self.ring.NodeLeave(v)
				break
			}
		}

	} else {
		if utils.IndexOf(self.currentMembers, self.config.Addr) < 0 {
			// self join
			self.handleSelfJoin(members)
			self.ring.NodeJoin(self.config.Addr)
			self.ready = true
		} else {
			// other join
			for _, v := range members {
				if utils.IndexOf(self.currentMembers, v) < 0 {
					self.ring.NodeJoin(v)
					break
				}
			}
		}
	}
	self.currentMembers = members
	self.memberLock.Unlock()
}

func (self *binBack) handleNodeLeave(oldMembers []string, whoLeft string) {
	// this is the last node, no need to migrate
	if len(oldMembers) == 2 {
		return
	}

	i := utils.IndexOf(oldMembers, whoLeft)
	i += len(oldMembers)
	prev := oldMembers[(i - 1) % len(oldMembers)]
	next := oldMembers[(i + 1) % len(oldMembers)]
	nNext := oldMembers[(i + 2) % len(oldMembers)]

	if self.config.Addr == next {
		prevClient, ok := self.backClients[prev]
		if !ok {
			prevClient = NewBackClient(prev)
			self.backClients[prev] = prevClient
		}

		data := ServerData{D: nil}
		_ = prevClient.GetPrimaryData(&ServerDataRequest{Addr: prev, Members: oldMembers, Delete: false}, &data)

		for k := range data.D {
			lock := self.getLockForKey(k)
			lock.Lock()

			for _, v := range data.D[k] {
				succ := false
				_ = self.config.Store.ListAppend(store.KV(k, v), &succ)
			}

			lock.Unlock()
		}

	} else if self.config.Addr == nNext {
		nextClient, ok := self.backClients[next]
		if !ok {
			nextClient = NewBackClient(next)
			self.backClients[next] = nextClient
		}

		data := ServerData{D: nil}
		_ = nextClient.GetPrimaryData(&ServerDataRequest{Addr: whoLeft, Members: oldMembers, Delete: false}, &data)

		for k := range data.D {
			lock := self.getLockForKey(k)
			lock.Lock()

			for _, v := range data.D[k] {
				succ := false
				_ = self.config.Store.ListAppend(store.KV(k, v), &succ)
			}

			lock.Unlock()
		}
	}
}

func (self *binBack) handleSelfJoin(newMembers []string) {
	i := utils.IndexOf(newMembers, self.config.Addr)
	i += len(newMembers)
	prev := newMembers[(i - 1) % len(newMembers)]
	next := newMembers[(i + 1) % len(newMembers)]
	nNext := newMembers[(i + 2) % len(newMembers)]

	nextClient, ok := self.backClients[next]
	if !ok {
		nextClient = NewBackClient(next)
		self.backClients[next] = nextClient
	}

	// get prev's primary data from next
	// next currently holds the copy
	// Delete next's copy if it is not the same as prev
	data := ServerData{D: nil}
	_ = nextClient.GetPrimaryData(&ServerDataRequest{Addr: prev, Members: newMembers, Delete: prev != next}, &data)

	for k := range data.D {
		lock := self.getLockForKey(k)
		lock.Lock()

		for _, v := range data.D[k] {
			succ := false
			_ = self.config.Store.ListAppend(store.KV(k, v), &succ)
		}

		lock.Unlock()
	}

	// if nNext == self, then there is only two servers alive
	// if there is only two copies, do not Delete
	deleteNNext := true
	if nNext == self.config.Addr {
		nNext = prev
		deleteNNext = false
	}

	nNextClient, ok := self.backClients[nNext]
	if !ok {
		nNextClient = NewBackClient(nNext)
		self.backClients[nNext] = nNextClient
	}

	// get self's primary data from nNext
	// nNext currently holds the copy
	data = ServerData{D: nil}
	_ = nNextClient.GetPrimaryData(&ServerDataRequest{Addr: self.config.Addr, Members: newMembers, Delete: deleteNNext}, &data)

	for k := range data.D {
		lock := self.getLockForKey(k)
		lock.Lock()

		for _, v := range data.D[k] {
			succ := false
			_ = self.config.Store.ListAppend(store.KV(k, v), &succ)
		}

		lock.Unlock()
	}
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

	i := utils.IndexOf(self.currentMembers, self.config.Addr) + 1
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

func (self *binBack) applyListDelete(log *Log) int {
	l := store.List{L: nil}
	_ = self.config.Store.ListGet(log.Key, &l)
	logs, _ := ParseListLog(l.L)

	n := 0
	for _, lg := range logs {
		if lg.Clock >= log.Clock {
			break
		}
		if lg.Clock < log.Clock && lg.Value == log.Value && lg.Operation == bin_config.ListLogAppend {
			d := 0
			s, _ := utils.ObjectToString(lg)
			_ = self.config.Store.ListRemove(store.KV(log.Key, s), &d)
			n += d
		}
	}

	return n
}

func (self *binBack) determineIfPrimary(addr string, bin string, members []string) bool {
	p := self.ring.GetPrimaryForKeyInView(bin, members)
	return addr == p
}

func (self *binBack) forwardReadRequest(bin string, operation string, input interface{}, output interface{}) error {
	target := ""
	if self.determineIfPrimary(self.config.Addr, bin, self.currentMembers) {
		target = self.currentMembers[(utils.IndexOf(self.currentMembers, self.config.Addr) + 1) % len(self.currentMembers)]
	} else {
		target = self.currentMembers[(utils.IndexOf(self.currentMembers, self.config.Addr) - 1) % len(self.currentMembers)]
	}

	client, ok := self.backClients[target]
	if !ok {
		client = NewBackClient(target)
		self.backClients[target] = client
	}

	return client.callOperation(operation, input, output)
}

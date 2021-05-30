package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/synchronization"
	"BinStorageZK/src/utils/node_ring"
	"fmt"
	"github.com/go-zookeeper/zk"
	"sort"
	"time"
)

type binClient struct {
	group *synchronization.GroupMember
	clients map[string]*binSingle
	bins map[string]*bin
	keepers []string
	currentMembers []string
	ring *node_ring.NodeRing
	backs []string
}

func NewBinClient(keepers []string, backs []string) *binClient {
	bClient := new(binClient)
	bClient.clients = make(map[string]*binSingle, 0)
	bClient.bins = make(map[string]*bin)
	bClient.group = nil
	bClient.keepers = keepers
	bClient.backs = backs

	return bClient
}

func (self *binClient)Bin(name string) store.Storage {
	b, ok := self.bins[name]
	if ok {
		return b
	}

	b = NewBin(name, self)
	self.bins[name] = b

	return b
}

func (self *binClient) getBinSingleForBin(name string, replica bool) (*binSingle, error) {
	if self.group == nil {
		conn, _, e := zk.Connect(self.keepers, time.Second)
		if e != nil {
			return nil, e
		}

		self.group = synchronization.NewGroupMember(conn, bin_config.GroupPath, "")
		members, e := self.group.GetCurrentMembers()
		if e != nil {
			return nil, e
		}
		sort.Strings(members)
		self.currentMembers = members
		self.ring = node_ring.NewNodeRing(self.backs, members)

		go func() {
			_ = self.group.Listen(func(members []string) {
				sort.Strings(members)
				self.currentMembers = members
				self.ring = node_ring.NewNodeRing(self.backs, members)
			})

			// if Listen returns, set this to nil so that it retries next time
			self.group = nil
		}()
	}

	if len(self.currentMembers) == 0 {
		return nil, fmt.Errorf("NO BACKEND FOUND")
	}

	i := 0
	if replica {
		i = 1
	}
	addr := self.ring.GetIthForKey(name, i)

	c, ok := self.clients[addr]
	if !ok {
		c = NewBinSingle(addr)
		self.clients[addr] = c
	}

	return c, nil
}

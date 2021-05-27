package bin_client

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/synchronization"
	"BinStorageZK/src/utils"
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
}

func NewBinClient(keepers []string) *binClient {
	binClient := new(binClient)
	binClient.clients = make(map[string]*binSingle, 0)
	binClient.bins = make(map[string]*bin)
	binClient.group = nil
	binClient.keepers = keepers

	return binClient
}

func (self *binClient)Bin(name string) store.Storage {
	b, ok := self.bins[name]
	if ok {
		return b
	}

	bin := NewBin(name, self)
	self.bins[name] = bin

	return bin
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

		go func() {
			_ = self.group.Listen(func(members []string) {
				sort.Strings(members)
				self.currentMembers = members
			})

			// if Listen returns, set this to nil so that it retries next time
			self.group = nil
		}()
	}

	if len(self.currentMembers) == 0 {
		return nil, fmt.Errorf("NO BACKEND FOUND")
	}

	h := utils.StringToFnvNumber(name)
	if replica {
		h += 1
	}
	addr := self.currentMembers[h % len(self.currentMembers)]

	c, ok := self.clients[addr]
	if !ok {
		c = NewBinSingle(addr)
		self.clients[addr] = c
	}

	return c, nil
}

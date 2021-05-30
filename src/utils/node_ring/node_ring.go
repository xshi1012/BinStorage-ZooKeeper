package node_ring

import (
	"BinStorageZK/src/utils"
	"sort"
	"sync"
)

type NodeRing struct {
	mtx sync.Mutex
	allNodes []string
	status map[string]bool
	current []string
}

func NewNodeRing(allNodes []string,  current []string) *NodeRing {
	n := new(NodeRing)

	n.allNodes = append(make([]string, 0, len(allNodes)), allNodes...)
	n.current = append(make([]string, 0, len(current)), current...)
	sort.Strings(n.allNodes)
	sort.Strings(n.current)

	n.status = make(map[string]bool)
	for  _, k := range allNodes {
		n.status[k] = false
	}
	for _, k := range current {
		n.status[k] = true
	}

	return n
}

func (self *NodeRing) NodeJoin(node string) {
	self.mtx.Lock()
	defer self.mtx.Unlock()

	alive, _ := self.status[node]
	if alive {
		return
	}

	self.status[node] = true
	self.current = utils.Unique(append(self.current, node))

	sort.Strings(self.current)
}

func (self *NodeRing) NodeLeave(node string) {
	self.mtx.Lock()
	defer self.mtx.Unlock()

	alive, _ := self.status[node]
	if !alive {
		return
	}

	self.status[node] = false
	j := -1
	for i, v := range self.current {
		if v == node {
			j = i
			break
		}
	}

	if j == -1 {
		return
	}

	self.current = append(self.current[:j], self.current[j+1:]...)
}

func (self *NodeRing) GetPrimaryForKey(key string) string {
	return self.GetIthForKey(key, 0)
}

func (self *NodeRing) GetIthForKey(key string, i int) string {
	self.mtx.Lock()
	defer self.mtx.Unlock()

	h := utils.StringToFnvNumber(key)
	p := ""
	for j := 0; j < len(self.allNodes); j++ {
		alive, _ := self.status[self.allNodes[(h + j) % len(self.allNodes)]]
		if alive {
			p = self.allNodes[(h + j) % len(self.allNodes)]
			break
		}
	}

	j := utils.IndexOf(self.current, p)
	return self.current[(j + i) % len(self.current)]
}

func (self *NodeRing) GetPrimaryForKeyInView(key string, current []string) string {
	view := NewNodeRing(self.allNodes, current)
	return view.GetPrimaryForKey(key)
}



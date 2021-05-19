package synchronization

import (
	"github.com/go-zookeeper/zk"
	"strings"
)

type GroupMember struct {
	groupPath string
	selfName string
	conn *zk.Conn
}

func NewGroupMember(conn *zk.Conn, groupPath string, selfName string) *GroupMember {
	g := new(GroupMember)
	g.conn = conn
	g.groupPath = groupPath
	g.selfName = selfName

	return g
}

func (self *GroupMember) Join() (string, error) {
	exist, _, e := self.conn.Exists(self.groupPath)
	if e != nil {
		return "", e
	} else if !exist {
		_, e = createRecursive(self.conn, self.groupPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if e != nil {
			return "", e
		}
	}

	s, e := CreateProtectedEphemeral(self.conn, self.groupPath + "/" + self.selfName, []byte{}, zk.WorldACL(zk.PermAll))

	return s, e
}

func (self *GroupMember) Listen(callback func([]string)) error {
	children, _, event, e := self.conn.ChildrenW(self.groupPath)
	if e != nil {
		return e
	}

	for ;; {
		if zk.EventNodeChildrenChanged  == (<-event).Type  {
			children, _, event, e = self.conn.ChildrenW(self.groupPath)
			if e != nil {
				return e
			}

			for i, v := range children {
				children[i] = strings.Split(v, "-")[1]
			}

			go callback(children)
		}
	}
}

func (self *GroupMember) GetCurrentMembers() ([]string, error) {
	children, _, e := self.conn.Children(self.groupPath)
	if e != nil {
		return nil, e
	}

	for i, v := range children {
		children[i] = strings.Split(v, "-")[1]
	}

	return children, e
}

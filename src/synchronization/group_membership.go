package synchronization

import (
	"github.com/go-zookeeper/zk"
)

func join (conn *zk.Conn, groupPath string, selfName string) (string, error){
	s, e := conn.CreateProtectedEphemeralSequential(groupPath + "/" + selfName, []byte(selfName), zk.WorldACL(zk.PermAll))
	return s, e
}

func listen (conn *zk.Conn, groupPath string, callback func([]string)) error{
	children, _, event, e := conn.ChildrenW(groupPath)
	for ;; {
		if zk.EventNodeChildrenChanged  == (<-event).Type  {
			children, _, event, e = conn.ChildrenW(groupPath)

			go callback(children)
		}
	}
	return e
}
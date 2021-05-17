package synchronization

import (
	"github.com/go-zookeeper/zk"
	"strings"
)

func createRecursive(conn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	name, e := conn.Create(path, data, flags, acl)
	if e == zk.ErrNoNode {
		parts := strings.Split(path, "/")
		pth := ""
		for _, p := range parts[1:] {
			var exists bool
			pth += "/" + p
			exists, _, e = conn.Exists(pth)
			if e != nil {
				return "", nil
			} else if exists {
				continue
			}

			_, e = conn.Create(pth, []byte{}, 0, acl)
			if e != nil && e != zk.ErrNodeExists {
				return "", e
			}
		}
	} else if e != nil {
		return "", e
	}

	return name, e
}
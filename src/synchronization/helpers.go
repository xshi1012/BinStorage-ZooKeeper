package synchronization

import (
	"crypto/rand"
	"fmt"
	"github.com/go-zookeeper/zk"
	"io"
	"strings"
)

const (
	protectedPrefix = "_c_"
)
func createRecursive(conn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	name, e := conn.Create(path, data, flags, acl)
	if e == zk.ErrNoNode {
		parts := strings.Split(path, "/")
		pth := ""
		for i, p := range parts[1:] {
			var exists bool
			pth += "/" + p
			exists, _, e = conn.Exists(pth)
			if e != nil {
				return "", nil
			} else if exists {
				continue
			}

			b := []byte{}
			if i == len(parts) - 2 {
				b = data
			}

			_, e = conn.Create(pth, b, 0, acl)
			if e != nil && e != zk.ErrNodeExists {
				return "", e
			}
		}
	} else if e != nil {
		return "", e
	}

	return name, e
}

func CreateProtectedEphemeral(conn *zk.Conn, path string, data []byte, acl []zk.ACL) (string, error) {
	var guid [16]byte
	_, e := io.ReadFull(rand.Reader, guid[:16])
	if e != nil {
		return "", e
	}
	guidStr := fmt.Sprintf("%x", guid)

	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s%s-%s", protectedPrefix, guidStr, parts[len(parts)-1])
	rootPath := strings.Join(parts[:len(parts)-1], "/")
	protectedPath := strings.Join(parts, "/")

	var newPath string

	for i := 0; i < 3; i++ {
		newPath, e = conn.Create(protectedPath, data, zk.FlagEphemeral, acl)
		switch e {
		case zk.ErrSessionExpired:
		case zk.ErrConnectionClosed:
			children, _, e := conn.Children(rootPath)
			if e != nil {
				return "", e
			}

			for _, p := range children {
				parts := strings.Split(p, "/")
				if pth := parts[len(parts)-1]; strings.HasPrefix(pth, protectedPrefix) {
					if g := pth[len(protectedPrefix) : len(protectedPrefix)+32]; g == guidStr {
						return rootPath + "/" + p, nil
					}
				}
			}
		case nil:
			return newPath, e
		default:
			return "", e
		}
	}
	return "", e
}

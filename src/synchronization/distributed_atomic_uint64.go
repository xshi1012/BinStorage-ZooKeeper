package synchronization

import (
	"encoding/binary"
	"fmt"
	"github.com/go-zookeeper/zk"
)

type DistriButedAtomicUint64 struct {
	conn *zk.Conn
	name string
	path string
	lock *zk.Lock
}

func NewDistributedAtomicLong(conn *zk.Conn, name string, path string) *DistriButedAtomicUint64 {
	l := new(DistriButedAtomicUint64)
	l.conn = conn
	l.name = name
	l.path = path
	l.lock = zk.NewLock(conn, fmt.Sprintf("%s/%s", path, name), zk.WorldACL(zk.PermAll))

	return l
}

func (self *DistriButedAtomicUint64) Init() error {
	fullPath := fmt.Sprintf("%s/%s", self.path, self.name)
	exist, _, e := self.conn.Exists(fullPath)
	if e != nil {
		return e
	} else if exist {
		return nil
	}

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(1))
	_, e = createRecursive(self.conn, fullPath, b, 0, zk.WorldACL(zk.PermAll))
	if e == zk.ErrNodeExists {
		return nil
	}
	return e
}

func (self *DistriButedAtomicUint64) GetAndIncrement() (uint64, error) {
	e := self.lock.Lock()
	if e != nil {
		return 0, e
	}
	defer self.lock.Unlock()

	fullPath := fmt.Sprintf("%s/%s", self.path, self.name)
	b, _, e := self.conn.Get(fullPath)
	if e != nil {
		return 0, e
	}
	v := binary.LittleEndian.Uint64(b)
	newV := make([]byte, 8)
	binary.LittleEndian.PutUint64(newV, v + 1)
	_, e = self.conn.Set(fullPath, newV, -1)
	if e != nil {
		return 0, e
	}

	return v, nil
}
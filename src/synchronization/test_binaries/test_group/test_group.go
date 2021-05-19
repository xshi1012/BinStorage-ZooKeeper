package main

import (
	"BinStorageZK/src/synchronization"
	"fmt"
	"github.com/go-zookeeper/zk"
	"os"
	"time"
)

func main() {
	conn, _, e := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if e != nil {
		panic(e)
	}

	g := synchronization.NewGroupMember(conn, "/trib", os.Args[1])
	s, e := g.Join()
	if e != nil {
		panic(e)
	}
	fmt.Println("Self path: " + s)

	currentGroup, e := g.GetCurrentMembers()
	if e != nil {
		panic(e)
	}

	fmt.Println("Existing group members (including self): ", currentGroup)

	ch := make(chan bool)

	go func() {
		e = g.Listen(func(names []string) {
			currentGroup = names
			ch <- true
		})

		panic(e)
	}()

	for range ch {
		fmt.Println(currentGroup)
	}
}

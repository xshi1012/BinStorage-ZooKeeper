package main

import (
	"BinStorageZK/src/synchronization"
	"fmt"
	"github.com/go-zookeeper/zk"
	"time"
)

func main() {
	finish := make(chan bool)
	go test_clock(finish)
	go test_clock(finish)

	<-finish
	<-finish
}

func test_clock(finish chan<- bool) {
	conn, _, e := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if e != nil {
		panic(e)
	}

	clock := synchronization.NewDistributedAtomicLong(conn, "clock", "/clock")
	e = clock.Init()
	if e != nil {
		panic(e)
	}

	for i := 0; i < 10; i++ {
		v, e := clock.GetAndIncrement()
		if e != nil {
			panic(e)
		} else {
			fmt.Println(v)
		}
	}

	finish <- true
}
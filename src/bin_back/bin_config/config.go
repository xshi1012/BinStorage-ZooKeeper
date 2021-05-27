package bin_config

import (
	"BinStorageZK/src/bin_back/store"
)

// Backend config
type BackConfig struct {
	Addr  string        // listen address
	Store store.Storage // the underlying storage it should use
	Ready chan<- bool   // send a value when server is ready
	Keepers []string    // list of zookeeper servers
}

const (
	Delimiter = "::"
	GroupPath = "/trib/backs"
	ServiceName = "BinBackRPC"
	ServiceClocksPath = "/trib/clocks"
	ServiceClockName = "clock"

	OperationClock = ServiceName + ".Clock"
	OperationGet = ServiceName + ".Get"
	OperationSet = ServiceName + ".Set"

	BackOperationForward = ServiceName + ".ForwardLog"

	ListLogAppend = "Append"
	ListLogDelete = "Delete"

	ListLog = "List"
	KeyValueLog = "KeyValue"
)

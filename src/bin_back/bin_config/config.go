package bin_config

import (
	"BinStorageZK/src/bin_back/store"
)

// Backend config
type BackConfig struct {
	Addr    string        // listen address
	Store   store.Storage // the underlying storage it should use
	Ready   chan<- bool   // send a value when server is ready
	Keepers []string      // list of zookeeper servers
	Backs   []string
}

const (
	Delimiter         = "::"
	GroupPath         = "/trib/backs"
	ServiceName       = "BinBackRPC"
	ServiceClocksPath = "/trib/clocks"
	ServiceClockName  = "clock"

	OperationClock      = ServiceName + ".Clock"
	OperationGet        = ServiceName + ".Get"
	OperationSet        = ServiceName + ".Set"
	OperationKeys       = ServiceName + ".Keys"
	OperationListGet    = ServiceName + ".ListGet"
	OperationListAppend = ServiceName + ".ListAppend"
	OperationListRemove = ServiceName + ".ListRemove"
	OperationListKeys   = ServiceName + ".ListKeys"

	BackOperationForward        = ServiceName + ".ForwardLog"
	BackOperationGetPrimaryData = ServiceName + ".GetPrimaryData"

	ListLogAppend = "Append"
	ListLogDelete = "Delete"

	ListLog     = "List"
	KeyValueLog = "KeyValue"
)

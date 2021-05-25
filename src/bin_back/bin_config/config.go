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
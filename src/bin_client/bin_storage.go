package bin_client

import "BinStorageZK/src/bin_back/store"

// Key-Storage interface
type BinStorage interface {
	// Fetch a storage based on the given bin_back name.
	Bin(name string) store.Storage
}

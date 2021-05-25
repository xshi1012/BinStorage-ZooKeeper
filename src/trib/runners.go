package trib

import "BinStorageZK/src/bin_client"

func NewFrontServer(s bin_client.BinStorage) Server {
	return NewFront(s)
}

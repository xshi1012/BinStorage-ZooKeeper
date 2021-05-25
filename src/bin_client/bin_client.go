package bin_client

import "BinStorageZK/src/bin_back/store"

type BinClient struct {

}

func NewBinClient(backs []string, keepers []string) *BinClient {
	panic("todo")
}

func (self *BinClient)Bin(name string) store.Storage {
	panic("todo")
}

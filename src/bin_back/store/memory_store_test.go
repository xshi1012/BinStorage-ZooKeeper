package store

import (
	"BinStorageZK/src/bin_back/store/storetest"
	"testing"
)

func TestStorage(t *testing.T) {
	s := NewMemoryStorage()

	storetest.CheckStorage(t, s)
}

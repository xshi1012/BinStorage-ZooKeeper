package bin_back

import "BinStorageZK/src/bin_back/store"

/*
 * interface for BinBackRPC
 * defines all exposed methods of binBack
 */
type BinBackRPC interface {
	Clock(_ uint64, ret *uint64) error
	Get(key string, value *string) error
	Set(kv *store.KeyValue, succ *bool) error

	ForwardLog(log *Log, succ *bool) error
}

package bin_back

/*
 * interface for BinBackRPC
 * defines all exposed methods of binBack
 */
type BinBackRPC interface {
	Clock(_ uint64, ret *uint64) error
}

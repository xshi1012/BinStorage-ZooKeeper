package store

type Storage interface {
	// Returns an auto-incrementing clock. The returned value of each call will
	// be unique, no smaller than atLeast, and strictly larger than the value
	// returned last time, unless it was math.MaxUint64.
	Clock(atLeast uint64, ret *uint64) error

	KeyString
	KeyList
}

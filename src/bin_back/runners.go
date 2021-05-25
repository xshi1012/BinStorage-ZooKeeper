package bin_back

import "BinStorageZK/src/bin_back/bin_config"

// Serve as a backend based on the given configuration
func ServeBack(b *bin_config.BackConfig) error {
	return NewBinBack(b).Run()
}

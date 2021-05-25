// Tribbler back-end launcher.
package main

import (
	"BinStorageZK/src/bin_back"
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/utils/local"
	"BinStorageZK/src/utils/ready"
	"flag"
	"fmt"
	"log"
	"strconv"
)

var (
	frc       = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
	verbose   = flag.Bool("v", false, "verbose logging")
	readyAddr = flag.String("ready", "", "ready notification address")
)

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	store.Logging = *verbose

	rc, e := bin_config.LoadRC(*frc)
	noError(e)

	run := func(i int) {
		if i > len(rc.Backs) {
			noError(fmt.Errorf("back-end index out of range: %d", i))
		}

		backConfig := rc.BackConfig(i, store.NewMemoryStorage())

		if *readyAddr != "" {
			backConfig.Ready = ready.Chan(*readyAddr, backConfig.Addr)
		}

		log.Printf("bin storage back-end serving on %s", backConfig.Addr)
		noError(bin_back.ServeBack(backConfig))
	}

	args := flag.Args()

	n := 0
	if len(args) == 0 {
		// scan for addresses on this machine
		for i, b := range rc.Backs {
			if local.Check(b) {
				go run(i)
				n++
			}
		}

		if n == 0 {
			log.Fatal("no back-end found for this host")
		}
	} else {
		// scan for indices for the addresses
		for _, a := range args {
			i, e := strconv.Atoi(a)
			noError(e)
			go run(i)
			n++
		}
	}

	if n > 0 {
		select {}
	}
}

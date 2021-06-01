package main

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_client"
	"BinStorageZK/src/trib"
	"BinStorageZK/src/trib/tribtest"
	"flag"
	"log"
	"testing"
)

var (
	frc       = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
	verbose   = flag.Bool("v", false, "verbose logging")
	readyAddr = flag.String("ready", "", "ready notification address")
)

func ne(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func makeServer() trib.Server {

	rc, e := bin_config.LoadRC(*frc)
	ne(e)

	c := bin_client.NewBinClient(rc.Keepers, rc.Backs)

	return trib.NewFrontServer(c)
}

func TestServer(t *testing.T) {
	server := makeServer()
	tribtest.CheckServer(t, server)
}

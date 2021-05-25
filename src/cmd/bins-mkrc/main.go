package main

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/utils/randaddr"
	"flag"
	"fmt"
	"log"
	"strings"
)

var (
	ips     = flag.String("ips", "localhost", "comma-seperated list of IP addresses of the set of machines that'll host backends and keepers")
	nback   = flag.Int("nback", 1, "number of back-ends")
	nkeep   = flag.Int("nkeep", 1, "number of keepers")
	frc     = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
	deflt   = flag.Bool("default", false, "default setup of 3 back-ends and 1 keeper")
	fixPort = flag.Bool("fix", false, "fix port numbers; don't use random ones")
)

func main() {
	flag.Parse()

	if *nback > 300 {
		log.Fatal(fmt.Errorf("too many back-ends"))
	}
	if *nkeep > 10 {
		log.Fatal(fmt.Errorf("too many keepers"))
	}

	if *deflt {
		*nback = 3
		*nkeep = 1
	}

	p := 3000
	if !*fixPort {
		p = randaddr.RandPort()
	}

	rc := new(bin_config.RC)
	rc.Backs = make([]string, *nback)
	rc.Keepers = make([]string, *nkeep)

	ip_addrs := strings.Split(*ips, ",")
	if nmachine := len(ip_addrs); nmachine > 0 {
		for i := 0; i < *nback; i++ {
			host := fmt.Sprintf("%s", ip_addrs[i%nmachine])
			rc.Backs[i] = fmt.Sprintf("%s:%d", host, p)
			p++
		}

		for i := 0; i < *nkeep; i++ {
			host := fmt.Sprintf("%s", ip_addrs[i%nmachine])
			rc.Keepers[i] = fmt.Sprintf("%s:%d", host, p)
			p++
		}
	}

	fmt.Println(rc.String())

	if *frc != "" {
		e := rc.Save(*frc)
		if e != nil {
			log.Fatal(e)
		}
	}
}

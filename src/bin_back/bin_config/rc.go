package bin_config

import (
	"BinStorageZK/src/bin_back/store"
	"encoding/json"
	"fmt"
	"os"
)

var DefaultRCPath = "bins.rc"

type RC struct {
	Backs   []string
	Keepers []string
}

type BackAddr struct {
	Serve string
	Peer  string
}

func (self *RC) BackCount() int {
	return len(self.Backs)
}

func (self *RC) BackConfig(i int, s store.Storage) *BackConfig {
	ret := new(BackConfig)
	ret.Addr = self.Backs[i]
	ret.Store = s
	ret.Ready = make(chan bool, 1)
	ret.Keepers = self.Keepers

	return ret
}

func LoadRC(p string) (*RC, error) {
	fin, e := os.Open(p)
	if e != nil {
		return nil, e
	}
	defer fin.Close()

	ret := new(RC)
	e = json.NewDecoder(fin).Decode(ret)
	if e != nil {
		return nil, e
	}

	return ret, nil
}

func (self *RC) marshal() []byte {
	b, e := json.MarshalIndent(self, "", "    ")
	if e != nil {
		panic(e)
	}

	return b
}

func (self *RC) Save(p string) error {
	b := self.marshal()

	fout, e := os.Create(p)
	if e != nil {
		return e
	}

	_, e = fout.Write(b)
	if e != nil {
		return e
	}

	_, e = fmt.Fprintln(fout)
	if e != nil {
		return e
	}

	return fout.Close()
}

func (self *RC) String() string {
	b := self.marshal()
	return string(b)
}

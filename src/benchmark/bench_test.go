package main

import (
	"BinStorageZK/src/bin_back/bin_config"
	"BinStorageZK/src/bin_client"
	"BinStorageZK/src/trib"
	"flag"
	"log"
	"strings"
	"testing"

	"github.com/lithammer/shortuuid"
)

var (
	frc       = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
	users     = make([]string, 0)
	server    = makeServer()
	ptr       = 0
	ptr2      = 0
	max_idx   = 99
	demo_user = "fenglu"
	demo_msg  = "Hello World"
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

func BenchmarkSignUp(b *testing.B) {
	server.SignUp(demo_user)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := shortuuid.New()
		a := "a" + strings.ToLower(id[:10])
		ne(server.SignUp(a))
		users = append(users, a)
	}
}

func BenchmarkListUsers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		server.ListUsers()
	}
}

func BenchmarkPost(b *testing.B) {
	clk := uint64(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ne(server.Post(demo_user, demo_msg, clk))
	}
}

func BenchmarkTribs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = server.Tribs(demo_user)
	}
}

func BenchmarkFollow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ne(server.Follow(demo_user, users[ptr]))
		ptr++
		if ptr > max_idx {
			ptr = 0
		}
	}
}

func BenchmarkUnFollow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ne(server.Unfollow(demo_user, users[ptr]))
		ptr++
		if ptr > max_idx {
			ptr = 0
		}
	}
}

func BenchmarkIsFollowing(b *testing.B) {
	for i := 0; i < (max_idx+1)/2; i++ {
		ne(server.Follow(demo_user, users[ptr2]))
		ptr2++
		if ptr2 > max_idx {
			ptr2 = 0
		}
	}

	for i := 0; i < b.N; i++ {
		server.IsFollowing(demo_user, users[ptr])
		ptr++
		if ptr > max_idx {
			ptr = 0
		}
	}
}

func BenchmarkFollowing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		server.Following(demo_user)
	}
}

func BenchmarkHome(b *testing.B) {
	clk := uint64(0)
	for i := 0; i <= (max_idx+1)/2; i++ {
		for j := 0; j <= max_idx; j++ {
			server.Post(users[ptr], demo_msg, clk)
		}
		ptr++
		if ptr > max_idx {
			ptr = 0
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Home(demo_user)
	}
}

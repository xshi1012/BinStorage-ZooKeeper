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
	frc    = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
	users  = make([]string, 0)
	server = makeServer()
	ptr    = 0
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
	server.SignUp("fenglu")

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
		server.Post("fenglu", "hello, world", clk)
	}
}

func BenchmarkTribs(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = server.Tribs("fenglu")
	}
}

func BenchmarkFollow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ne(server.Follow("fenglu", users[ptr]))
		ptr++
	}
}

// func BenchmarkUnFollow(b *testing.B) {
// 	server := makeServer()
// 	//clk := uint64(0)
// 	server.SignUp("fenglu")
// 	var s []string
// 	for i := 0; i < b.N; i++ {
// 		id := shortuuid.New()
// 		a := "a" + strings.ToLower(id[:10])
// 		//fmt.Println(a + " " + strconv.Itoa(b.N))
// 		s = append(s, a)
// 		ne(server.SignUp(a))
// 	}

// 	for i := 0; i < b.N; i++ {
// 		server.Follow("fenglu", s[i])
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.Unfollow("fenglu", s[i])
// 	}
// }

// func BenchmarkIsFollowing(b *testing.B) {
// 	server := makeServer()
// 	//clk := uint64(0)
// 	server.SignUp("fenglu")
// 	var s []string
// 	for i := 0; i < b.N; i++ {
// 		id := shortuuid.New()
// 		a := "a" + strings.ToLower(id[:10])
// 		//fmt.Println(a + " " + strconv.Itoa(b.N))
// 		s = append(s, a)
// 		ne(server.SignUp(a))
// 	}

// 	for i := 0; i < b.N; i++ {
// 		server.Follow("fenglu", s[i])
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.IsFollowing("fenglu", s[i])
// 	}
// }

// func BenchmarkFollowing(b *testing.B) {
// 	server := makeServer()
// 	//clk := uint64(0)
// 	server.SignUp("fenglu")
// 	var s []string
// 	for i := 0; i < b.N; i++ {
// 		id := shortuuid.New()
// 		a := "a" + strings.ToLower(id[:10])
// 		//fmt.Println(a + " " + strconv.Itoa(b.N))
// 		s = append(s, a)
// 		ne(server.SignUp(a))
// 	}

// 	for i := 0; i < b.N; i++ {
// 		server.Follow("fenglu", s[i])
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.Following("fenglu")
// 	}
// }

// func BenchmarkHome(b *testing.B) {
// 	server := makeServer()
// 	clk := uint64(0)
// 	server.SignUp("fenglu")
// 	var s []string
// 	for i := 0; i < 3; i++ {
// 		id := shortuuid.New()
// 		a := "a" + strings.ToLower(id[:10])
// 		//fmt.Println(a + " " + strconv.Itoa(b.N))
// 		s = append(s, a)
// 		ne(server.SignUp(a))
// 	}

// 	for i := 0; i < 3; i++ {
// 		for j := 0; j < 100; j++ {
// 			server.Post(s[i], "hello, world"+strconv.Itoa(j), clk)
// 		}
// 	}

// 	for i := 0; i < 100; i++ {
// 		server.Post("fenglu", "hello, world"+strconv.Itoa(i), clk)
// 	}

// 	for i := 0; i < 3; i++ {
// 		server.Follow("fenglu", s[i])
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.Home("fenglu")
// 	}
// }

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
	frc = flag.String("rc", bin_config.DefaultRCPath, "bin storage config file")
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

// func BenchmarkSignUp(b *testing.B) {
// 	server := makeServer()
// 	//fmt.Println(strings.ToLower(fake.FirstName()))

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		id := shortuuid.New()
// 		a := "a" + strings.ToLower(id[:10])
// 		ne(server.SignUp(a))
// 	}
// }

// func BenchmarkListUsers(b *testing.B) {
// 	server := makeServer()
// 	//fmt.Println(b.N)
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.ListUsers()
// 	}
// }

// func BenchmarkPost(b *testing.B) {
// 	server := makeServer()
// 	clk := uint64(0)
// 	server.SignUp("h8liu")

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.Post("h8liu", "hello, world"+strconv.Itoa(b.N), clk)
// 	}
// }

// func BenchmarkTribs(b *testing.B) {
// 	server := makeServer()
// 	clk := uint64(0)
// 	server.SignUp("fenglu")

// 	for i := 0; i < 100; i++ {
// 		server.Post("fenglu", "hello, world"+strconv.Itoa(i), clk)
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		_, _ = server.Tribs("fenglu")
// 	}
// }

// func BenchmarkFollow(b *testing.B) {
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

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		server.Follow("fenglu", s[i])
// 	}
// }

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

func BenchmarkHome(b *testing.B) {
	server := makeServer()
	clk := uint64(0)
	server.SignUp("fenglu")
	var s []string
	for i := 0; i < b.N; i++ {
		id := shortuuid.New()
		a := "a" + strings.ToLower(id[:10])
		//fmt.Println(a + " " + strconv.Itoa(b.N))
		s = append(s, a)
		ne(server.SignUp(a))
	}

	for i := 0; i < b.N; i++ {
		server.Post(s[i], "hello, world", clk)
	}

	server.Post("fenglu", "hello, world", clk)

	for i := 0; i < b.N; i++ {
		server.Follow("fenglu", s[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Home("fenglu")
	}
}

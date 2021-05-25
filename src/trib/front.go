package trib

import (
	"BinStorageZK/src/bin_back/store"
	"BinStorageZK/src/bin_client"
	"BinStorageZK/src/utils"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	UsersBin      = "USERS"
	SignUpKey     = "REGISTERED_USERS"
	TribsKey      = "TRIBS"
	FollowKey     = "FOLLOWING::"
	Follow        = "FOLLOWING"
	Unfollow      = "UNFOLLOWING"
	NumOfUserBins = 10
)

/**
 * Front: An implementation of trib.Server
 */
type Front struct {
	BinStorage bin_client.BinStorage
	Users *store.List
}

type FollowLog struct {
	Operation string
	Clock uint64
}

func NewFront(binStorage bin_client.BinStorage) *Front {
	f := new(Front)
	f.BinStorage = binStorage
	f.Users = new(store.List)

	return f
}

func (self *Front) userExists(user string) (bool, error) {
	if !IsValidUsername(user) {
		return false, fmt.Errorf("INVALID USER NAME")
	}

	for _, v := range self.Users.L {
		if v == user {
			return true, nil
		}
	}

	res := store.List{L: nil}
	binString := UsersBin + strconv.Itoa(utils.StringToFnvNumber(user) % NumOfUserBins)

	client := self.BinStorage.Bin(binString)
	e := client.ListGet(SignUpKey, &res)
	if e != nil {
		return false, e
	}
	self.Users.L = append(self.Users.L, res.L...)
	self.Users.L = utils.Unique(self.Users.L)

	for _, v := range self.Users.L {
		if v == user {
			return true, nil
		}
	}

	return false, nil
}

func (self *Front) deletePosts(user string, tribs []*Trib) error {
	client := self.BinStorage.Bin(user)

	for _, t := range tribs {
		s, e := TribToString(t)
		if e != nil {
			return e
		}

		n := 0
		e = client.ListRemove(&store.KeyValue{Key: TribsKey, Value: s}, &n)
		if e != nil {
			return e
		}
	}
	return nil
}

func (self *Front) SignUp(user string) error {
	exist, e := self.userExists(user)
	if e != nil {
		return e
	} else if exist {
		return fmt.Errorf("USER ALREADY EXISTS")
	}

	binString := UsersBin + strconv.Itoa(utils.StringToFnvNumber(user) % NumOfUserBins)
	client := self.BinStorage.Bin(binString)

	succ := false
	kv := store.KeyValue{Key: SignUpKey, Value: user}
	e = client.ListAppend(&kv, &succ)
	if e != nil {
		return e
	} else if !succ {
		return fmt.Errorf("SOMETHING WENT WRONG")
	}

	return nil
}

func (self *Front) ListUsers() ([]string, error) {
	if len(self.Users.L) >= MinListUser {
		sort.Strings(self.Users.L)
		return append([]string(nil), self.Users.L...), nil
	}

	for i := 0; i < NumOfUserBins; i++ {
		binString := UsersBin + strconv.Itoa(i)
		client := self.BinStorage.Bin(binString)
		res := store.List{L:nil}

		e := client.ListGet(SignUpKey, &res)
		if e != nil {
			return nil, e
		}

		self.Users.L = append(self.Users.L, res.L...)
		self.Users.L = utils.Unique(self.Users.L)

		if len(self.Users.L) >= MinListUser {
			break;
		}
	}

	sort.Strings(self.Users.L)
	return append([]string(nil), self.Users.L...), nil
}

func (self *Front) Post(who, post string, clock uint64) error {
	t := time.Now()

	// check if user exists
	exist, e := self.userExists(who)
	if e != nil {
		return e
	} else if !exist {
		return fmt.Errorf("USER DOES NOT EXIST")
	}

	// check if post is too long
	if len(post) > MaxTribLen {
		return fmt.Errorf("THIS POST IS TOO LONG")
	}

	client := self.BinStorage.Bin(who)

	var c uint64 = 0
	e = client.Clock(clock, &c)
	if e != nil {
		return e
	}

	content := Trib{User: who, Message: post, Time: t, Clock: c}
	b, e := TribToString(&content)
	if e != nil {
		return e
	}

	succ := false
	kv := &store.KeyValue{Key: TribsKey, Value: b}
	e = client.ListAppend(kv, &succ)
	if e != nil {
		return e
	} else if !succ {
		return fmt.Errorf("POST FAILED")
	}

	return nil
}

func (self *Front) Tribs(user string) ([]*Trib, error) {
	// check if user exists
	exist, e := self.userExists(user)
	if e != nil {
		return nil, e
	} else if !exist {
		return nil, fmt.Errorf("USER DOES NOT EXIST")
	}

	client := self.BinStorage.Bin(user)

	out := store.List{L: nil}

	e = client.ListGet(TribsKey, &out)
	if e != nil {
		return nil, e
	}

	tribs := make([]*Trib, 0, len(out.L))
	for _, v := range out.L {
		t, e := StringToTrib(v)
		if e != nil {
			return nil, e
		}
		tribs = append(tribs, t)
	}

	sort.Sort(TribList(tribs))

	if len(tribs) > MaxTribFetch {
		garbage := tribs[:len(tribs) - MaxTribFetch]
		go func(){
			err := self.deletePosts(user, garbage)
			if err != nil {
				log.Println("Failed to delete posts: " + err.Error())
			}
		}()
		tribs = tribs[len(tribs) - MaxTribFetch:]
	}

	return tribs, nil
}

func (self *Front) Follow(who, whom string) error {
	// check if same user
	if who == whom {
		return fmt.Errorf("INPUT USERS ARE THE SAME")
	}

	// check if the users exist
	exist, e := self.userExists(who)
	if e != nil {
		return e
	} else if !exist {
		return fmt.Errorf("THE FIRST USER DOES NOT EXIST")
	}

	exist, e = self.userExists(whom)
	if e != nil {
		return e
	} else if !exist {
		return fmt.Errorf("THE SECOND USER DOES NOT EXIST")
	}

	following, e := self.IsFollowing(who, whom)
	if e != nil {
		return e
	} else if following {
		return fmt.Errorf("ALREADY FOLLOWING")
	}

	client := self.BinStorage.Bin(who)

	f, e := self.Following(who)
	if e != nil {
		return e
	} else if len(f) >= MaxFollowing {
		return fmt.Errorf("YOU CAN FOLLOW AT MOST %d USERS", MaxFollowing)
	}

	succ := false
	kv := store.KeyValue{Key: FollowKey + whom, Value: Follow}
	e = client.ListAppend(&kv, &succ)
	if e != nil {
		return e
	}

	if !succ {
		return fmt.Errorf("SOMETHING WENT WRONG")
	}

	return nil
}

func (self *Front) Unfollow(who, whom string) error {
	// check if same user
	if who == whom {
		return fmt.Errorf("INPUT USERS ARE THE SAME")
	}

	// check if the users exist
	exist, e := self.userExists(who)
	if e != nil {
		return e
	} else if !exist {
		return fmt.Errorf("THE FIRST USER DOES NOT EXIST")
	}

	exist, e = self.userExists(whom)
	if e != nil {
		return e
	} else if !exist {
		return fmt.Errorf("THE SECOND USER DOES NOT EXIST")
	}

	client := self.BinStorage.Bin(who)

	unfollowed := 0
	kv := store.KeyValue{Key: FollowKey + whom, Value: Follow}
	e = client.ListRemove(&kv, &unfollowed)
	if e != nil {
		return e
	}

	if unfollowed == 0 {
		return fmt.Errorf("NOT FOLLOWING")
	}

	return nil
}

func (self *Front) IsFollowing(who, whom string) (bool, error) {
	// check if same user
	if who == whom {
		return false, fmt.Errorf("INPUT USERS ARE THE SAME")
	}

	// check if the users exist
	exist, e := self.userExists(who)
	if e != nil {
		return false, e
	} else if !exist {
		return false, fmt.Errorf("THE FIRST USER DOES NOT EXIST")
	}

	exist, e = self.userExists(whom)
	if e != nil {
		return false, e
	} else if !exist {
		return false, fmt.Errorf("THE SECOND USER DOES NOT EXIST")
	}

	client := self.BinStorage.Bin(who)

	out := store.List{L: nil}
	e = client.ListGet(FollowKey + whom, &out)
	if e != nil {
		return false, e
	}

	if len(out.L) > 0 && out.L[0] == Follow {
		return true, nil
	}
	 return false, nil
}

func (self *Front) Following(who string) ([]string, error) {
	// check if user exists
	exist, e := self.userExists(who)
	if e != nil {
		return nil, e
	} else if !exist {
		return nil, fmt.Errorf("USER DOES NOT EXIST")
	}

	client := self.BinStorage.Bin(who)

	out := store.List{L: nil}
	p := store.Pattern{Prefix: FollowKey, Suffix: ""}
	e = client.ListKeys(&p, &out)
	if e != nil {
		return nil, e
	}

	ret := make([]string, 0, len(out.L))
	for _, v := range out.L {
		ret = append(ret, v[len(FollowKey):])
	}

	return ret, nil
}

func (self *Front) Home(user string) ([]*Trib, error) {
	exist, e := self.userExists(user)
	if e != nil {
		return nil, e
	} else if !exist {
		return nil, fmt.Errorf("USER DOES NOT EXIST")
	}

	tribs := make([]*Trib, 0)

	following, e := self.Following(user)
	if e != nil {
		return nil, e
	}

	following = append(following, user)
	lock := &sync.Mutex{}
	done := make(chan bool, len(following))

	for _, u := range following {
		go func(user string) {
			got, e := self.Tribs(user)
			if e != nil {
				done <- false
			}

			lock.Lock()
			tribs = append(tribs, got...)
			lock.Unlock()
			done <- true

		}(u)
	}

	for i := 0; i < len(following); i++ {
		if !<-done {
			return nil, fmt.Errorf("ERROR FETCHING TRIBS")
		}
	}

	sort.Sort(TribList(tribs))

	if len(tribs) > MaxTribFetch {
		return tribs[len(tribs) - MaxTribFetch:], nil
	}

	return tribs, nil
}

package triblab_test

import (
	"testing"
	"triblab"

	"trib/entries"
	"trib/randaddr"
	"trib/store"
	"trib"
/*	"runtime/debug"
	"sort"
	"strconv"
	"time"
	"fmt"*/
)

func TestServerExtended(t *testing.T) {
	addr := randaddr.Local()
	ready := make(chan bool)
	go func() {
		e := entries.ServeBackSingle(addr, store.NewStorage(), ready)
		if e != nil {
			t.Fatal(e)
		}
	}()

	<-ready
	client := triblab.NewBinClient([]string{addr})
	server := entries.MakeFrontSingle(addr)

	MyCheckServer(t, server, client)
}

func MyCheckServer(t *testing.T, server trib.Server, client trib.BinStorage) {
	/*
	ne := func(e error) {
		if e != nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	er := func(e error) {
		if e == nil {
			debug.PrintStack()
			t.Fatal(e)
		}
	}

	as := func(cond bool) {
		if !cond {
			debug.PrintStack()
			t.Fatal()
		}
	}

	ne(server.SignUp("dlisuk"))
	for i:=0; i < 2000; i++{
		start := time.Now()
		ne(server.SignUp("dlisuk"+strconv.FormatInt(int64(i),10)))
		end := time.Now()
		as(end.Sub(start).Seconds() < 3)
	}

	users, e := server.ListUsers()
	ne(e)

	as(len(users) >= 20)
	sort.Strings(users)
	users2, e := server.ListUsers()
	for i := range users2{
		as(users[i] == users2[i])
	}



	for i:=0; i < 100; i++{
		start := time.Now()
		ne(server.Follow("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10)))
		b, e := server.IsFollowing("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10))
		end := time.Now()
		ne(e)
		as(b)
		as(end.Sub(start).Seconds() < 6)
	}
	er(server.Follow("dlisuk", "dlisuk0"))

	for i:=0; i < 100; i++{
		start := time.Now()
		ne(server.Unfollow("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10)))
		b, e := server.IsFollowing("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10))
		end := time.Now()
		ne(e)
		as(!b)
		as(end.Sub(start).Seconds() < 6)
	}
	for i:=200; i < 300 ; i++{
		start := time.Now()
		ne(server.Follow("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10)))
		b, e := server.IsFollowing("dlisuk", "dlisuk"+strconv.FormatInt(int64(i),10))
		end := time.Now()
		ne(e)
		as(b)
		as(end.Sub(start).Seconds() < 6)
	}
	start := time.Now()
	followees, err := server.Following("dlisuk")
	end := time.Now()
	as(end.Sub(start).Seconds() < 6)
	ne(err)
	for i:=200; i < 300; i++{
		as(followees[i-200] == "dlisuk" + strconv.FormatInt(int64(i),10))
	}


	var clk uint64
	clk = 0
	for i:=200; i < 300 ; i++{
		user :=  "dlisuk"+strconv.FormatInt(int64(i),10)
		start := time.Now()
		var tribs trib.List
		client.Bin(user).ListGet("_TRIBS_",&tribs)
		fmt.Printf("%d tribs before\n",len(tribs.L))

		for j:=0; j < 1000; j++{
			msg := "msg" + strconv.FormatInt(int64(j),36)
			server.Post(user,msg,clk)
		}
		end := time.Now()
		as(end.Sub(start).Seconds() < 3000)
		client.Bin(user).ListGet("_TRIBS_",&tribs)
		fmt.Printf("%d tribs after\n",len(tribs.L))
	}
	for i:=200; i < 300 ; i++{
		user :=  "dlisuk"+strconv.FormatInt(int64(i),10)
		start := time.Now()
		tribs,err := server.Tribs(user)
		end := time.Now()
		as(end.Sub(start).Seconds() < 3)
		ne(err)
		as(len(tribs) == trib.MaxTribFetch)
	}

	tribs, e := server.Tribs("dlisuk200")
	ne(e)
	as(len(tribs) == trib.MaxTribFetch)
	tr := tribs[0]
	if tr.Clock > clk {
		clk = tr.Clock
	}

	tribs, e = server.Home("dlisuk")
	ne(e)
	as(tribs != nil)
	as(len(tribs) == trib.MaxTribFetch)
	for _,t := range tribs{
		fmt.Printf("%v %v %v %v\n",t.Clock,t.Time,t.User,t.Message)
	}
	*/
}

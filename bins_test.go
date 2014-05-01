package triblab_test

import (
	"fmt"
	"testing"

	"trib"
	"trib/entries"
	"trib/randaddr"
	"trib/store"
	"trib/tribtest"
	"triblab"
)

func TestBinStorage(t *testing.T) {
	addr1 := "localhost:10000"
	addr2 := "localhost:11001"
	addr3 := "localhost:10050"

	for addr2 == addr1 {
		addr2 = randaddr.Local()
	}

	ready1 := make(chan bool)
	ready2 := make(chan bool)
	ready3 := make(chan bool)

	run := func(addr string, ready chan bool) {
		e := entries.ServeBackSingle(addr,
			store.NewStorage(), ready)
		if e != nil {
			t.Fatal(e)
		}
	}

	go run(addr1, ready1)
	go run(addr2, ready2)
	go run(addr3, ready3)

	r := <-ready1 && <-ready2 && <-ready3
	if !r {
		t.Fatal("not ready")
	}

	readyk1 := make(chan bool)
	readyk2 := make(chan bool)
	addrk1 := "localhost:12000"
	addrk2 := "localhost:12050"

	go func() {
		e := triblab.ServeKeeper(&trib.KeeperConfig{
			Backs: []string{addr1, addr2, addr3},
			Addrs: []string{addrk1, addrk2},
			This:  0,
			Id:    0,
			Ready: readyk1,
		})
		if e != nil {
			t.Fatal(e)
		}
	}()

	go func() {
		e := triblab.ServeKeeper(&trib.KeeperConfig{
			Backs: []string{addr1, addr2, addr3},
			Addrs: []string{addrk1, addrk2},
			This:  1,
			Id:    64,
			Ready: readyk2,
		})
		if e != nil {
			t.Fatal(e)
		}
	}()

	r = <-readyk1 && <-readyk2

	if !r {
		t.Fatal("keeper not ready")
	}

	bc := triblab.NewBinClient(
		[]string{addr1, addr2, addr3},
	)

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		c := bc.Bin(fmt.Sprintf("b%d", i))
		go func(s trib.Storage) {
			tribtest.CheckStorage(t, s)
			done <- true
		}(c)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

package triblab

import (
	"trib"
	"time"
	"net/rpc"
	"log"
	"strings"
)

func ServeKeeper(kc *trib.KeeperConfig) error {
	backs :=  make([]trib.Storage, 0, len(kc.Backs))
	for i := range kc.Backs{
		backs = append(backs, NewClient(kc.Backs[i]))
	}

	/*
		Update peer keeper's info, and current keeper's id & address
	*/
	keeper_addrs := make([]string, len(kc.Addrs)-1)
	this_id := kc.This
	this_address := ""
	// addr_mapping := make(map[string]trib.Storage)

	keeper_addr_hash_mapping := make(map[uint64]string)
	back_addr_hash_mapping := make(map[uint64]string)

	primary_back_replica_mapping := make(map[string]string)
	_ = this_id

	//locate current keeper's address
	j := 0
	for i, _ := range kc.Addrs {
		if i == kc.This {
			this_address = kc.Addrs[i]
			continue
		}
		keeper_addrs[j] = kc.Addrs[i]
		j++
	}

	//calculate current keeper's hash
	keeper_hash := hashBinKey(this_address)
	log.Print("current keeper's hash --->", keeper_hash)

	//calculate all keeper's hash & ip address mapping
	for i := range kc.Addrs {
		keeper_addr_hash_mapping[hashBinKey(kc.Addrs[i])] = kc.Addrs[i]
	}

	//calculate all backend's hash & ip address mapping
	for i := range kc.Backs{
		back_addr_hash_mapping[hashBinKey(kc.Backs[i])] = kc.Backs[i]
	}

	//sort backend address hash
	back_addr_hash_pair_list := sortBackAddrByHash(back_addr_hash_mapping)
	log.Print("backend primary hash and ip address mapping --->", back_addr_hash_pair_list)

	//establish replica mapping
	len_hash_pair := len(back_addr_hash_pair_list)
	i := 0
	for ; i < len_hash_pair - 1
	{
		primary_back_replica_mapping[back_addr_hash_pair_list[i].Address] = back_addr_hash_pair_list[i+1].Address
		i += 1
	}
	primary_back_replica_mapping[back_addr_hash_pair_list[len_hash_pair - 1].Address] = back_addr_hash_pair_list[0].Address
	log.Print("backend primary and its replica ip address mapping --->", primary_back_replica_mapping)

	// backend ip address & backend instance mapping
	// for _, addr := range kc.Backs {
	// 	addr_mapping[addr] = NewClient(addr)
	// }

	if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

	var highest uint64
	ticker  := time.NewTicker(time.Second)
	errChan := make(chan error)
	seenClocks := make(chan uint64)

	/*
		clock synchronization with all backends
	*/

	go func(tick <-chan time.Time){
		for {
			_ = <- tick
			go func() {
				for i := range backs {
					go func(back trib.Storage) {
						var ret uint64
						err := back.Clock(highest, &ret)
						if err != nil && err != rpc.ErrShutdown{
							errChan <- err
						}
						seenClocks <- ret
					}(backs[i])
				}
				var maxClock uint64
				for _ = range backs {
					nextClock := <-seenClocks
					if nextClock > maxClock{
						maxClock = nextClock
					}
				}
				if maxClock > highest{
					highest = maxClock
				}else{
					highest = highest + 1
				}
			}()
		}
	}(ticker.C)

	/*
		Keeper polling log from backend
	*/
	go func() {
		for {
			for k,v := range primary_back_replica_mapping {
				c, err := getConnection(k)
				_, _, _ = c, v, err
				// Do we need to maintain the connection for all the other primary backends?
			}

		}
	}()

	err := <- errChan
	return err
}

func getConnection(addr string)(*rpc.Client, error) {
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil && strings.Contains(err.Error(), "connection refused") {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

// func KeeperHeartBeat(b *trib.BackConfig) error {
// 	s := rpc.NewServer()
// 	s.RegisterName("Storage", b.Store)
// 	listener, err := net.Listen("tcp",b.Addr)

// 	if err != nil{
// 		if b.Ready != nil { go func(ch chan<- bool) { ch <- false } (b.Ready) }
// 		return err
// 	}

// 	if b.Ready != nil { go func(ch chan<- bool ) { ch <- true } (b.Ready) }
// 	return http.Serve(listener, s)
// }

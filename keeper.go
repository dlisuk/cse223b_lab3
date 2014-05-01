package triblab

import (
    "trib"
    "time"
    "net/rpc"
    "log"
    // "strings"
)

func duplicate(Backup string, cmd string, kv *trib.KeyValue, succ *bool) {
    backup := NewClient(Backup)
    switch {
    case cmd == "Set":
        backup.Set(kv, succ)
    case cmd == "ListAppend":
        backup.ListAppend(kv, succ)
    case cmd == "ListRemove":
        var n int
        backup.ListRemove(kv,&n)
    }
}
func ServeKeeper(kc *trib.KeeperConfig) error {
    backs :=  make([]trib.Storage, 0, len(kc.Backs))
    for i := range kc.Backs{
        backs = append(backs, NewClient(kc.Backs[i]))
    }


    //go routine
    

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
    keeper_hash := HashBinKey(this_address)
    log.Print("current keeper's hash --->", keeper_hash)

    //calculate all keeper's hash & ip address mapping
    for i := range kc.Addrs {
        keeper_addr_hash_mapping[HashBinKey(kc.Addrs[i])] = kc.Addrs[i]
    }

    //calculate all backend's hash & ip address mapping
    for i := range kc.Backs{
        back_addr_hash_mapping[HashBinKey(kc.Backs[i])] = kc.Backs[i]
    }

    //sort backend address hash
    back_addr_hash_pair_list := sortBackAddrByHash(back_addr_hash_mapping)
    log.Print("backend primary hash and ip address mapping --->", back_addr_hash_pair_list)

    //establish replica mapping
    len_hash_pair := len(back_addr_hash_pair_list)
    i := 0
    //primary_back_replica_mapping : [primary ip, replica ip]
    for ; i < len_hash_pair - 1
    {
        primary_back_replica_mapping[back_addr_hash_pair_list[i].Address] = back_addr_hash_pair_list[i+1].Address
        i += 1
    }
    //hook up the tail with the head
    primary_back_replica_mapping[back_addr_hash_pair_list[len_hash_pair - 1].Address] = back_addr_hash_pair_list[0].Address
    log.Print("backend primary and its replica ip address mapping --->", primary_back_replica_mapping)

    // backend ip address & backend instance mapping
    // for _, addr := range kc.Backs {
    //  addr_mapping[addr] = NewClient(addr)
    // }

    if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

    var highest uint64
    ticker  := time.NewTicker(time.Second)
    errChan := make(chan error)
    seenClocks := make(chan uint64)

    /*
        clock synchronization with all backends
    */

    master := make(chan bool)

    //determine master keeper
    go func(){
        master <- true
    }()

    go func(master chan bool){
        <- master
        log.Println("master is true")
        //if this keeper is the master
        go func(tick <-chan time.Time){
            for {
                _ = <- tick
                go func() {
                    log.Println("clock is being synced")
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
        
    }(master)


    



    /*
        Keeper polling log from backend
    */
    go func() {
        for {
            time.Sleep(1 * time.Millisecond)
            for k,v := range primary_back_replica_mapping {
                var rawList trib.List
                primary := NewClient(k)
                replica := NewClient(v)
                // _,_,_ = primary,replica, rawList

                err := primary.ListGet(LogKey,&rawList)
                if err!=nil{
                    //placeholder
                }
                for _,cmd := range rawList.L{
                    //RPC call
                    //Set  ListAppend  ListRemove
                    // func ExtractCmd(cmd string) (string, *trib.KeyValue, error){
                    var n int
                    replicaErr := execute(replica,cmd)
                    if replicaErr == nil{
                        //execute on the primary
                        primaryErr := execute(primary,cmd)
                        if primaryErr != nil{
                            //remove log 
                            log_kv := trib.KV(LogKey, cmd)
                            err = primary.ListRemove(log_kv, &n)
                            //client will be unblocked
                            if n!=1{
                                //placeholder
                            }
                            if err!=nil {
                                //placeholder
                            }
                        }
                    }
                }

            }

        }
    }()

    err := <- errChan
    return err
}

func execute(backend trib.Storage, cmd string) error{
    op, kv, _ := ExtractCmd(cmd) //confirm with david
    var succ bool
    var err error
    var n int
    switch op{
        case "Set":
            err = backend.Set(kv,&succ)
        case "ListAppend":
            err = backend.ListAppend(kv, &succ)
        case "ListRemove":
            err = backend.ListRemove(kv, &n)
    }

    if err != nil{
        return err
    }
        
    return nil
}



package triblab

import (
  "trib"
  "time"
  "net/rpc"
  "log"
  "trib/store"
  "strings"
  // "strconv"
)

type keeper struct{
  Addr string
  Connection *rpc.Client
}

func (self *keeper) getConnection() (*rpc.Client, error) {
  c := self.Connection
  var err error = nil
  if c == nil {
      c, err = rpc.DialHTTP("tcp", self.Addr)
      self.Connection = c
      if err != nil && strings.Contains(err.Error(), "connection refused") {
          return nil, nil
      }
      if err != nil {
          return nil, err
      }
  }
  return c, nil
}


// func (self *keeper) isMaster(flag *bool) error{
  // c, err := self.getConnection()
  // if c != nil {

  // }
  // if err != nil && err == rpc.ErrShutdown{
  //     self.Connection = nil
  // }
  // return err
// }

// func (self *keeper) getID(id *int) error{
//     fields := strings.split(self.Addr, "::")
//     if len(fields) != 2
//         return errors.New("not enough fields in the passed in addr")
//     *id = fields[1]
//     return nil


// }


// Creates an RPC client that connects to a keeper.
func NewKeeperConnection(addr string) keeperCommunicate {
  return &keeper{Addr:addr}
}

type keeperCommunicate interface{

}



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

  var num_of_alive_keeper int
  //go routine


  /*
      Update peer keeper's info, and current keeper's id & address
  */
  keeper_addrs := make([]string, len(kc.Addrs)-1)
  // keeper_index := kc.This
  this_address := ""
  // addr_mapping := make(map[string]trib.Storage)

  keeper_addr_hash_mapping := make(map[uint64]string)
  back_addr_hash_mapping := make(map[uint64]string)

  primary_back_replica_mapping := make(map[string]string)

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

  //create a new keeper back
  // Addr  string       listen address
  // Store Storage      the underlying storage it should use
  // Ready chan<- bool  send a value when server is ready

  go func(){
    ready := make(chan bool)
    // s :=
    // address := this_address+"::"+strconv.Itoa(keeper_index)
    back := trib.BackConfig{Addr:this_address, Store:store.NewStorage(), Ready:ready}
    ServeBack(&back)

  }()



  backs :=  make([]trib.Storage, 0, len(kc.Backs))
  for i := range kc.Backs{
    backs = append(backs, NewClient(kc.Backs[i]))
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
  master <- true

  //determine master keeper
  go func(tick <-chan time.Time){

    num_of_alive_keeper = 0

    for i, addr := range kc.Addrs {
      keeper_conn := keeper{addr,nil}
      _, err := keeper_conn.getConnection()
      if err == nil{
        //handle keeper up
        //count ++
        num_of_alive_keeper++
        //check the keeper's index with my index
        if i < kc.This {
            master <- false
        }
      }
    }

  }(ticker.C)



  //if this keeper is the master
  go func(tick <-chan time.Time, master chan bool){
    for {
      _ = <- tick
      <- master
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
  }(ticker.C, master)


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

        }//end cmd list loop
      } //end k,v for loop
    }// end for loop
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



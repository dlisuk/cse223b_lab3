package triblab

import (
  "trib"
  "time"
  "net/rpc"
  "log"
  "trib/store"
  "strings"
  "sort"
  // "strconv"
)

type keeper struct{
  Addr string
  This int
  Hash uint64
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
  // backend structs list
  backend_structs_list := make([]backend, 0, len(kc.Backs))
  // keeper structs list
  keeper_structs_list := make([]keeper, 0, len(kc.Addrs))

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

  //create keeper structs list
  for i := range kc.Addrs{
    keeper_structs_list = append(keeper_structs_list, keeper{Addr: kc.Addrs[i], This: i, Hash: HashBinKey(kc.Backs[i]), Connection: nil })
  }

  sort.Sort(keeperByHash(keeper_structs_list))
  log.Println(keeper_structs_list)

  //create backend structs list
  for i := range kc.Backs{
    backend_structs_list = append(backend_structs_list, backend{addr: kc.Backs[i], hash: HashBinKey(kc.Backs[i]), store: NewClient(kc.Backs[i])})
  }

  sort.Sort(byHash(backend_structs_list))
  log.Println(backend_structs_list)


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


  //calculate current keeper's hash
  keeper_hash := HashBinKey(this_address)
  log.Print("current keeper's hash --->", keeper_hash)


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

    for i, keeper := range keeper_structs_list {
      _, err := keeper.getConnection()
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
      // some <- master
      <- master
      go func() {
        log.Println("clock is being synced")
        for i := range backend_structs_list {
          go func(back trib.Storage) {
            var ret uint64
            err := back.Clock(highest, &ret)
            if err != nil && err != rpc.ErrShutdown{
                errChan <- err
            }
            seenClocks <- ret
          }(backend_structs_list[i].store)
        }
        var maxClock uint64
        for _ = range backend_structs_list {
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



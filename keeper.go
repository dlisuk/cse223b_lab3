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

type remoteKeeper struct{
  Addr string
  This int
  Hash uint64
  Connection *rpc.Client
}

func (self *remoteKeeper) HeartBeat(senderHash uint64, responseHash *uint64) error {
    c, err := self.getConnection()
    if err != nil && err == rpc.ErrShutdown{
        return err
    }
    if c != nil {
        //set the responseHash

         //Local Heartbeat:  func (self *localKeeper) HeartBeat(senderHash uint64, responseHash *uint64) error{
        err = c.Call("LocalKeeper.HeartBeat",senderHash,responseHash)      
        return err
    }

    return nil
}

func NewRemoteKeeper(addr string, this int) *remoteKeeper{
    //compute hash
    hash := HashBinKey(Addr)
    return &remoteKeeper{Addr:addr, 
                        This:this, 
                        Hash:hash, 
                        Connection:nil}
}


func (self *remoteKeeper) getConnection() (*rpc.Client, error) {
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

//This is the keeper which is running here
type localKeeper struct{
    index         int
	hash          uint64
	lowerBound    uint64
	remoteKeepers []keeper
	backends      []backend
	errChan       chan error
}

//This is tthe place where we send heart beats to remote keepers
func (self *localKeeper) pingNeighbor(){
    
    neighborIndex := (self.index +1)%len(remoteKeepers) 
    for{
        if neighborIndex == self.index{
            break;
        }
        remoteKeeper := remoteKeepers[neighborIndex]
        var responseHash int
        err := remoteKeeper.HeartBeat(self.Hash, &responseHash)
        if err != nil{
            neighborIndex = (neighborIndex+1)%len(remoteKeepers);
        }
        else{
            break;
        }
    }
}

//This is the function which calls the clock on all the backends if we are the master
func (self *localKeeper) clockManager(){
	var highest uint64
	ticker  := time.NewTicker(time.Second)
	errChan := make(chan error)
	seenClocks := make(chan uint64)

	/*
			clock synchronization with all backends
	*/

	//TODO: Make sure all this time stuff is correct
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
							self.errChan <- err
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
}

//This is the function that handles replication of master/slaves
func (self *localKeeper) replicationManager(){
	/*
			Keeper polling log from backend
	*/
	go func() {
		//This maps indexes of the backends struct list to index
		primary_back_replica_mapping := make(map[int]int)

		for {
			time.Sleep(1 * time.Millisecond)
			for k,v := range primary_back_replica_mapping {
				var rawList trib.List
				//TODO: instead of creaing a new client we should use the back list we have created earlier,
				//TODO:  we really don't need the primary_back_replica_mapping map
				//TODO: HERE IS THE CLIENT IN THE STRUCT: backend_structs_list[0].store.Get
				//TODO: We should mark the lower bound replica/lower bound master on the backends here
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
							//TODO: Add results log?

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

}

//This is the function that figures out when backends come up/go down and handles copying of all data from one backend to annother
func (self *localKeeper) backendManager(){

}


//This is the heart beat function we need to call through RPC
func (self *localKeeper) HeartBeat(senderHash uint64, responseHash *uint64) error{
	//TODO: we need to use the senderHash to figure out if we need to change our lower bound.
	//TODO: we then need to send what our lower bound is.
    if self.lowerBound == senderHash {
        //do nothing
    }
    else {
        self.lowerBound = senderHash
    }
    responseHash = self.lowerBound
    return nil
}


// Creates an RPC client that connects to a keeper.
func NewKeeperConnection(addr string) keeperCommunicate {
  return &keeper{Addr:addr}
}

type keeperCommunicate interface{

}

func (self *localKeeper) keeperServer() error {
    s := rpc.NewServer()
    s.RegisterName("LocalKeeper", self)
    listener, err := net.Listen("tcp",self.Addr)

    if err != nil{
        return err
    }
    return http.Serve(listener, s)
}


func ServeKeeper(kc *trib.KeeperConfig) error {


  //create keeper structs list
  keeper_structs_list := make([]keeper, 0, len(kc.Addrs))
  var this_keeper *keeper
  var this_index int
  for i := range kc.Addrs{
    keeper_structs_list = append(keeper_structs_list, keeper{Addr: kc.Addrs[i], This: i, Hash: HashBinKey(kc.Backs[i]), Connection: nil })
		if(i == kc.This){
			this_keeper = keeper_structs_list[i]
            this_index = i
		}
  }
  sort.Sort(keeperByHash(keeper_structs_list))
  log.Println(keeper_structs_list)
	log.Println(this_keeper)

  //create backend structs list
  backend_structs_list := make([]backend, 0, len(kc.Backs))
  for i := range kc.Backs{
    backend_structs_list = append(backend_structs_list, backend{addr: kc.Backs[i], hash: HashBinKey(kc.Backs[i]), store: NewClient(kc.Backs[i])})
  }
	sort.Sort(byHash(backend_structs_list))
	log.Println(backend_structs_list)
	errChan := make(chan error)

	keeper := &localKeeper{
        this_index
		this_keeper.Hash,
		this_keeper.Hash,
		keeper_structs_list,
		backend_structs_list,
		errChan}

	go keeper.pingNeighbor()
	go keeper.backendManager()
	go keeper.clockManager()
	go keeper.replicationManager()

  if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

  err := <- errChan

  return err
}

func execute(backend trib.Storage, cmd string) error{
  op, kv, err := ExtractCmd(cmd)
	if err != nil { return err }
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



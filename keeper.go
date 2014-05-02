package triblab

import (
  "trib"
  "time"
  "net/rpc"
  "log"
  "strings"
  "sort"
  "strconv"
  "math"
  "errors"
  "sync"
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
	hash           uint64
	lowerBound     uint64
	remoteKeepers  []remoteKeeper
	backends       []backendKeeper
	errChan        chan error
	masterFlag     bool
	replicators    map[int]int
	replicatorLock sync.Mutex
}
type rkByHash []backendKeeper
func (v rkByHash) Len() int { return len(v) }
func (v rkByHash) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v rkByHash) Less(i, j int) bool { return v[i].hash < v[j].hash }
type backendKeeper struct{
	addr      string
	hash      uint64
	store     trib.Storage
	replicator int
	up        bool
}

func (self *localKeeper) inRange(x uint64) bool{
	lb := self.lowerBound < x
	ub := x <= self.hash
	return lb && ub || (self.hash < self.lowerBound && (lb || ub))
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
//This is the function which calls the clock on all the backends if we are the master
func (self *localKeeper) clockManager(){
  ticker  := time.NewTicker(time.Second)
  /*
      clock synchronization with all backends
  */

  //TODO: Make sure all this time stuff is correct

  //determine master keeper
  go func(tick <-chan time.Time){

    var minimumHash uint64
    minimumHash = math.MaxUint64

    for _, keeper := range self.remoteKeepers {
      responseHash := new(uint64)
      err := keeper.HeartBeat(self.hash, responseHash)
      if err == nil{
        //handle keeper up
        //count ++
        //check the keeper's index with my index
        if *responseHash < minimumHash {
          minimumHash = *responseHash
        }
      }
    }
    if minimumHash == self.hash {
      self.masterFlag = true
    }
  }(ticker.C)
}

func (self *localKeeper) syncClock(){
    //if this keeper is the master
  var highest uint64
  ticker  := time.NewTicker(time.Second)
  seenClocks := make(chan uint64)

  go func(tick <-chan time.Time){
    for {
      _ = <- tick
      switch self.masterFlag{
        case true:
          go func() {
            log.Println("clock is being synced")
            for i := range self.backends {
              go func(back trib.Storage) {
                var ret uint64
                err := back.Clock(highest, &ret)
                if err != nil && err != rpc.ErrShutdown{
                  self.errChan <- err
                }
                seenClocks <- ret
              }(self.backends[i].store)
            }
            var maxClock uint64
            for _ = range self.backends {
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
        case false:
      }
    }
  }(ticker.C)
}

//This is the function that handles replication of master/slaves
//Also this detects server crashes
func (self *localKeeper) replicationManager(){

	//This maps indexes of the backends struct list to index
	for {
		time.Sleep(250 * time.Millisecond)
		self.replicatorLock.Lock()
		PR_Loop: for p,p_back := range self.backends {
			if p_back.up == false || !self.inRange(p_back.hash){
				continue
			}
			primary := p_back.store
			r       := p_back.replicator
			replica := self.backends[r].store

			var rawLog trib.List
			err := primary.ListGet(LogKey,&rawLog)
			if err!=nil{
				self.serverCrash(p)
				break PR_Loop
			}
			for _,cmd := range rawLog.L{
				var n int
				resr,err := execute(replica,cmd)
				if err != nil {
					self.serverCrash(r)
					break PR_Loop
				}else{
					//execute on the primary
					resp,err := execute(primary,cmd)
					if err != nil {
						self.serverCrash(r)
						break PR_Loop
					}else{
						log_kv := trib.KV(LogKey, cmd)
						err = primary.ListRemove(log_kv, &n)
						if err != nil {
							self.serverCrash(p)
							break PR_Loop
						}
						if n!=1{
							//TODO: What does this case mean, is it even possible?
						}
						if resr != resp {
							//TODO: Responses don't match, does this matter?
						}
						//Response log, make it a bit cleaner
						err = primary.ListAppend(trib.KV(ResLogKey,cmd + "::" + resp), nil)
						if err != nil {
							self.serverCrash(p)
							break PR_Loop
						}
					}
				}
			}//end cmd list loop
		} //end p,r for loop
		self.replicatorLock.Unlock()
	}// end infinite loop
}


func (self *localKeeper) serverCrash(index int){
	//Caller must have locked the replicator lock
	//TODO: Here we need to figure out what to do when a server goes down, make sure it's replicator can take over/such
	self.backends[index].up = false
}
func (self *localKeeper) serverJoin(index int){
	//Caller must have locked the replicator lock
	//TODO: Here we need to figure out what to do when a server comes up, make sure it's replicator can take over/such
	self.backends[index].up = true
}

//This is the function that figures out when backends come up
func (self *localKeeper) backendManager(){
	//Run forever and ever
	for {
		for i,back := range self.backends{
			//If it's marked as up, we don't care about it
			if back.up || !self.inRange(back.hash){
				continue
			}
			var res string
			err := back.store.Get(MasterKeyLB, &res)
			//This server is up now
			if err == nil{
				self.replicatorLock.Lock()
				self.serverJoin(i)
				self.replicatorLock.Unlock()
			}
		}
	}
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

  keeper_structs_list := make([]remoteKeeper, 0, len(kc.Addrs))
	var this_keeper *remoteKeeper
    var this_index int
  for i := range kc.Addrs{
    keeper_structs_list = append(keeper_structs_list, remoteKeeper{Addr: kc.Addrs[i], This: i, Hash: HashBinKey(kc.Backs[i]), Connection: nil })
		if(i == kc.This){
			this_keeper = &keeper_structs_list[i]
            this_index = i

		}
  }
  sort.Sort(keeperByHash(keeper_structs_list))
  log.Println("keeper structs list --->", keeper_structs_list)
	log.Println("current keeper --->", this_keeper)

  //create backend structs list
  backend_structs_list := make([]backendKeeper, 0, len(kc.Backs))
  for i := range kc.Backs{
    backend_structs_list = append(backend_structs_list, backendKeeper{addr: kc.Backs[i], hash: HashBinKey(kc.Backs[i]), store: NewClient(kc.Backs[i]), replicator:-1, up:false})
  }
	sort.Sort(rkByHash(backend_structs_list))
	log.Println("backend structs list --->", backend_structs_list)
	errChan := make(chan error)
	masterClock := false

	keeper := &localKeeper{
        this_index
		this_keeper.Hash,
		this_keeper.Hash,
		keeper_structs_list,
		backend_structs_list,
		errChan,
		masterClock,
		make(map[int]int),
		sync.Mutex{} }

  log.Println("start ping neighbor")
	go keeper.pingNeighbor()
  log.Println("start running backend manager")
	go keeper.backendManager()
  log.Println("start running replication manager")
	go keeper.replicationManager()


  log.Println("start clock manager")
  go keeper.clockManager()

  log.Println("start master clock daemon")
  go keeper.syncClock()


  if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

  err := <- errChan

  return err
}

func execute(backend trib.Storage, cmd string) error{
  op, kv, err := ExtractCmd(cmd)

	if err != nil { return "",err }
	response := ""
    switch op{
    case "Set":
			var succ bool
			err = backend.Set(kv,&succ)
			response = strconv.FormatBool(succ)
    case "ListAppend":
			var succ bool
			err = backend.ListAppend(kv, &succ)
			response = strconv.FormatBool(succ)
    case "ListRemove":
			var n int
			err = backend.ListRemove(kv, &n)
			response = strconv.FormatInt(int64(n),10)
		default:
			err = errors.New("Undefined operation: " + op)
  }
	return response,err
}



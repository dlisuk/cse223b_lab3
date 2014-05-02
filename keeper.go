package triblab

import (
  "trib"
  "time"
  "net/rpc"
  //"log"
  "strings"
  "sort"
  "strconv"
  "math"
  "errors"
  "sync"
  "net/http"
  "net"
)

type remoteKeeper struct{
  Addr string
  This int
  Hash uint64
  Connection *rpc.Client
}

func (self *remoteKeeper) HeartBeat(senderHash uint64, responseHash *uint64) error {
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("LocalKeeper.HeartBeat",senderHash,responseHash)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.Connection = nil
	}
	return err
}

func NewRemoteKeeper(addr string, this int) *remoteKeeper{
    //compute hash
    hash := HashBinKey(addr)
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
			return nil, err
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
	addr       string
	hash       uint64
	store      trib.Storage
	replicator int
	replicates int
	mlb        uint64
	rlb        uint64
	up         bool
}

func (self *localKeeper) inRange(x uint64) bool{
	lb := self.lowerBound < x
	ub := x <= self.hash
	return lb && ub || (self.hash < self.lowerBound && (lb || ub))
}

//This is tthe place where we send heart beats to remote keepers
func (self *localKeeper) pingNeighbor(){

    neighborIndex := (self.index +1)%len(self.remoteKeepers)

	for {
		for {
			if neighborIndex == self.index {
				break;
			}
			remoteKeeper := self.remoteKeepers[neighborIndex]
			var responseHash uint64
			err := remoteKeeper.HeartBeat(self.hash, &responseHash)
			if err != nil {
				neighborIndex = (neighborIndex+1)%len(self.remoteKeepers)
			}else {
				break
			}
		}
		time.Sleep(time.Second)
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
      *responseHash = uint64(1)
      err := keeper.HeartBeat(self.hash, responseHash)
      if err == nil{
        //handle keeper up
        //count ++
        //check the keeper's index with my index
        //log.Println("responseHash", *responseHash)
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
						//log.Println("Syncing clock " + strconv.Itoa(self.index))
            for i := range self.backends {
              go func(back trib.Storage) {
                var ret uint64
                err := back.Clock(highest, &ret)
                if err != nil && err != rpc.ErrShutdown{
                  //self.errChan <- err
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
		time.Sleep(time.Second)
		
		self.replicatorLock.Lock()
		PR_Loop: for p,p_back := range self.backends {
			if p_back.up == false || !self.inRange(p_back.hash){
				continue
			}
			//log.Println(strconv.FormatUint(self.lowerBound,10) + "-" + strconv.FormatUint(self.hash,10) + " is replicationg " + strconv.FormatUint(p_back.hash,10))

			// log.Println("replicationManager at work")
			primary := p_back.store
			r       := p_back.replicator

			replica := self.backends[r].store

			var rawLog trib.List
			err := primary.ListGet(LogKey,&rawLog)
			// log.Println("The log is : ", len(rawLog.L))
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
						var dummySucc bool
						err = primary.ListAppend(trib.KV(ResLogKey,cmd + "::" + resp), &dummySucc)
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
	//log.Println("SERVER CRASH " + strconv.Itoa(index))
	//Caller must have locked the replicator lock
	back := self.backends[index]
	//log.Println("CRASH ON " + back.addr)
	newMasterInd := back.replicator
	newMaster    := self.backends[newMasterInd]
	newSlaveInd  := newMaster.replicator
	newSlave     := self.backends[newSlaveInd]
	//We don't care if this backup is  down, someone else must have dealt with it
	allDoneCh := make(chan bool)

	var succ bool
	//Step 0
	//the new master can immediatly be the master, accept issueing but not committing commands
	_ = newMaster.store.Set(trib.KV(MasterKeyLB, strconv.FormatUint(back.mlb,10)), &succ)
	//log.Println(newMaster.addr + " is now master " + strconv.FormatUint(back.mlb,10))

	//First we want to initiate the copies since they are long running and can be backgrounded
	//crashed server master data -> new slave
	copy1ch := make(chan bool)
	go self.copy(newMasterInd,newSlaveInd, back.mlb, back.hash, copy1ch)
	go func(){
		var succ bool
		_ = <- copy1ch
		//The new slave is now officially a slave
		_ = newSlave.store.Set(trib.KV(ReplicKeyLB, strconv.FormatUint(back.mlb,10)), &succ)
		//log.Println(newSlave.addr + " is now slave1 " + strconv.FormatUint(back.mlb,10))
		allDoneCh <- true
	}()

	//crashed server slave data -> new master
	copy2ch := make(chan bool)
	go self.copy(back.replicates, newMasterInd, back.rlb, back.mlb, copy2ch)
	go func(){
		var succ bool
		_ = <- copy2ch
		//The new master is now officially a slave to the old master's master
		_ = newMaster.store.Set(trib.KV(ReplicKeyLB, strconv.FormatUint(back.rlb,10)), &succ)
		//log.Println(newMaster.addr + " mis now slave " + strconv.FormatUint(back.rlb,10))
		allDoneCh <- true
	}()

	_ = <- allDoneCh
	_ = <- allDoneCh
	//log.Println("SERVER Done " + strconv.Itoa(index))
}

func (self *localKeeper) serverJoin(index int){
	//log.Println("SERVER Join " + strconv.Itoa(index))
	//Caller must have locked the replicator lock
	//TODO: Here we need to figure out what to do when a server comes up, make sure it's replicator can take over/such

	predInd := -1
	succInd := -1
	for i := index -1; i >= 0; i -- {
		if self.backends[i].up{
			predInd = i
			break
		}
	}
	if predInd == -1{
		for i := len(self.backends)-1; i > index; i -- {
			if self.backends[i].up{
				predInd = i
				break
			}
		}
	}
	for i := index+1; i < len(self.backends); i ++ {
		if self.backends[i].up{
			succInd = i
			break
		}
	}
	if succInd == -1{
		for i := 0; i < index; i ++ {
			if self.backends[i].up{
				succInd = i
				break
			}
		}
	}
	if succInd == -1 || predInd == -1{
		//log.Println("FAILURE")
		return
	}

	pred 					:= &self.backends[predInd]
	joined_server := &self.backends[index]
	succ 					:= &self.backends[succInd]

	joined_server.replicates = predInd
	joined_server.replicator = succInd

	pred.replicator = index
	succ.replicates = index


  allDoneCh := make(chan bool)

	var passed bool
	//Step 0
	_ = succ.store.Set(trib.KV(ReplicKeyLB,strconv.FormatUint(succ.mlb, 10)), &passed)
	//Step 1
	_ = succ.store.Set(trib.KV(MasterKeyLB,strconv.FormatUint(joined_server.hash, 10)), &passed)
	//Step 2
	//TODO: FLUSH LOG OF SUCC , this has to block

	//Step 3
	_ = joined_server.store.Set(trib.KV(MasterKeyLB,strconv.FormatUint(succ.mlb,10)), &passed)


  //First we want to initiate the copies since they are long running and can be backgrounded
  //crashed server master data -> new slave
  copy1ch := make(chan bool)
	//Step 4
  go self.copy(succInd,index, pred.mlb,  joined_server.hash, copy1ch)
  go func(){
    _ = <- copy1ch
		//Step 5
		_ = joined_server.store.Set(trib.KV(ReplicKeyLB,strconv.FormatUint(succ.rlb,10)), &passed)
    allDoneCh <- true
  }()

	_ = <- allDoneCh

	//log.Println("SERVER Join DONE" + strconv.Itoa(index))
}

func (self *localKeeper) binInRange(lb uint64, ub uint64, key string) bool{
	fields  := strings.Split(key, "::")
	binName := fields[0]
	binHash := HashBinKey(binName)
	lbPass := lb < binHash
	ubPass := binHash <= ub
	return lbPass && ubPass || (ub < lb && (lbPass || ubPass) )
}

func (self *localKeeper) copy(s int, d int,lb uint64, ub uint64, done chan bool) error{
	source := self.backends[s]
	dest   := self.backends[d]

  p := &trib.Pattern{"",""}
  var list trib.List
  //copy all the lists
  err := source.store.ListKeys(p, &list)
  if err!=nil{return err}

  for _, key := range list.L{
    if !self.binInRange(lb,ub,key) || key == LogKey || key == ResLogKey || key == CommittedKey || key == MasterKeyLB || key == ReplicKeyLB{
    	 continue
    }else{
        //copy over
        list_item := trib.List{}
        err = source.store.ListGet(key, &list_item)
        if err!=nil{return err}
        for _, item := range list_item.L{
        	kv := trib.KV(key,item)
        	var succ bool
        	err = dest.store.ListAppend(kv,&succ)
        	if err != nil {return err}
        }
        // //remove from the source
        // var n int
        // kv := trib.KV(key, list_item)
        // err = source.store.ListRemove(kv, &n)
        // if err!=nil{return nil}
      }
  }

  //copy all the kv pair
  // var list trib.List
  err = source.store.Keys(p,&list)
  if err!=nil{return err}
  for _,key := range list.L{
    //copy over the kv
    if self.binInRange(lb,ub,key) == false || key == CommittedKey || key == MasterKeyLB || key == ReplicKeyLB{
    	continue
    }
		var value string
    err = source.store.Get(key, &value)
		if err!=nil{return err}
    kv := trib.KV(key,value)
    err = dest.store.Set(kv, nil)
  }
	if done != nil { go func(ch chan bool ) { ch <- true } (done) }
	return nil
}


//This is the function that figures out when backends come up
func (self *localKeeper) backendManager(){
	//Run forever and ever
	for {
		for i,back := range self.backends{
			var mlbS, rlbS string
			err1 := back.store.Get(MasterKeyLB, &mlbS)
			err2 := back.store.Get(ReplicKeyLB, &rlbS)

			var mlb, rlb uint64
			var up bool
			if err1 == nil && err2 == nil{
				mlb, _ = strconv.ParseUint(mlbS,10,64)
				rlb, _ = strconv.ParseUint(rlbS,10,64)
				up = true
			}else{
				mlb = 0
				rlb = 0
				up  = false
			}

			if back.up != up || back.mlb != mlb || back.rlb != rlb {
				self.replicatorLock.Lock()
				if back.up == false && up == true && self.inRange(back.hash){
					//If we are a manager of this we need to make it join, this will change the server side mlb/rlb
					self.serverJoin(i)
					_ = back.store.Get(MasterKeyLB, &mlbS)
					_ = back.store.Get(ReplicKeyLB, &rlbS)
				}
				if back.up == true && up == false && self.inRange(back.hash){
					//If we are a manager of this we need to make it join, this will change the server side mlb/rlb
					self.serverCrash(i)
				}
				self.backends[i].up  = up
				self.backends[i].mlb = mlb
				self.backends[i].rlb = rlb

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
    }else {
				//log.Printf("Recieved " + strconv.FormatUint(senderHash,10) + " on " + strconv.FormatUint(self.hash,10))
        self.lowerBound = senderHash
    }
    *responseHash = self.hash
    return nil
}

func (self *localKeeper) keeperServer() error {
    s := rpc.NewServer()
    s.RegisterName("LocalKeeper", self)
    listener, err := net.Listen("tcp",self.remoteKeepers[self.index].Addr)

    if err != nil{
        return err
    }
		//log.Println("Keeper Serving")
    return http.Serve(listener, s)

}

func ServeKeeper(kc *trib.KeeperConfig) error {


  //create keeper structs list

  keeper_structs_list := make([]remoteKeeper, 0, len(kc.Addrs))
	var this_keeper *remoteKeeper
    var this_index int
  for i := range kc.Addrs{
    keeper_structs_list = append(keeper_structs_list, remoteKeeper{Addr: kc.Addrs[i], This: i, Hash: HashBinKey(kc.Addrs[i]), Connection: nil })
		if(i == kc.This){
			this_keeper = &keeper_structs_list[i]
            this_index = i
		}
  }
  sort.Sort(keeperByHash(keeper_structs_list))
	for i := range keeper_structs_list{
		keeper_structs_list[i].This = i
	}
	this_index = this_keeper.This
	//log.Println("keeper structs list --->", keeper_structs_list)
	//log.Println("current keeper --->", this_keeper)

  //create backend structs list
  backend_structs_list := make([]backendKeeper, 0, len(kc.Backs))
  for i := range kc.Backs{
		hash := HashBinKey(kc.Backs[i])
    backend_structs_list = append(
			backend_structs_list,
			backendKeeper{
				addr: kc.Backs[i],
				hash: hash,
				store: NewClient(kc.Backs[i]),
				replicator:-1,
				replicates:-1,
				up:false})
  }
	sort.Sort(rkByHash(backend_structs_list))
	//log.Println("backend structs list --->", backend_structs_list)
	errChan := make(chan error)
	masterClock := false

	keeper := &localKeeper{
		this_index,
		this_keeper.Hash,
		this_keeper.Hash,
		keeper_structs_list,
		backend_structs_list,
		errChan,
		masterClock,
		make(map[int]int),
		sync.Mutex{} }
	//log.Println("local keeper")
	//log.Println(keeper)

	//We don't want to continue until at least 3 backends are up

	upBacks := 0
	for upBacks < 3 {
		time.Sleep(250)
		for i,b := range keeper.backends {
			var res string
			if(b.up){
				err := b.store.Get(MasterKeyLB,&res)
				if err != nil{
					keeper.backends[i].up = false
					upBacks = upBacks -1
				}
			}else{
				err := b.store.Get(MasterKeyLB,&res)
				if err == nil{
					keeper.backends[i].up = true
					upBacks = upBacks + 1
				}
			}
		}
	}


	upKeepers := make([]int,0)
	for i,b := range keeper.backends {
		if b.up{
			upKeepers = append(upKeepers, i)
		}
	}
	for i,b := range upKeepers{
		pred := i - 1
		if pred < 0 {pred = len(upKeepers)-1}
		predpred := pred -1
		if predpred < 0 {predpred = len(upKeepers)-1}
		succ := i + 1
		if len(upKeepers) <= succ  {succ = 0}
		pred = upKeepers[pred]
		succ = upKeepers[succ]
		keeper.backends[b].replicates = pred
		keeper.backends[b].mlb        = keeper.backends[pred].hash
		keeper.backends[b].replicator = succ
		keeper.backends[b].rlb        = keeper.backends[predpred].hash
		keeper.backends[b].store.Set(trib.KV(MasterKeyLB,strconv.FormatUint(keeper.backends[b].mlb,10)),nil)
		keeper.backends[b].store.Set(trib.KV(ReplicKeyLB,strconv.FormatUint(keeper.backends[b].rlb,10)),nil)
		/*log.Println("ADDR: " + keeper.backends[b].addr)
		log.Println(keeper.backends[b].replicates)
		log.Println(keeper.backends[b].mlb)
		log.Println(keeper.backends[b].replicator)
		log.Println(keeper.backends[b].rlb)*/

	}

	//start the keeper server
	go keeper.keeperServer()
	time.Sleep(2*time.Second)

	if(len(keeper.remoteKeepers) > 1) {
		//log.Println("start ping neighbor")
		go keeper.pingNeighbor()
	}else{
		keeper.masterFlag = true
	}
  //log.Println("start running backend manager")
	go keeper.backendManager()
  //log.Println("start running replication manager")
	go keeper.replicationManager()


  //log.Println("start clock manager")
  //keeper.clockManager()

  //log.Println("start master clock daemon")
  go keeper.syncClock()

	//log.Println("local keeper")
	//log.Println(keeper)

  if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

  err := <- errChan

  return err
}

func execute(backend trib.Storage, cmd string) (string,error){
  op, kv, err := ExtractCmd(cmd)

	if err != nil { return "",err }
	response := ""
    switch op{
    case "Storage.Set":
			var succ bool
			err = backend.Set(kv,&succ)
			response = strconv.FormatBool(succ)
    case "Storage.ListAppend":
			var succ bool
			err = backend.ListAppend(kv, &succ)
			response = strconv.FormatBool(succ)
    case "Storage.ListRemove":
			var n int
			err = backend.ListRemove(kv, &n)
			response = strconv.FormatInt(int64(n),10)
		default:
			err = errors.New("Undefined operation: " + op)
  }
	return response,err
}



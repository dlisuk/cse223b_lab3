package triblab

import (
  "trib"
  "time"
  "net/rpc"
  "log"
  "strings"
  "sort"
  "strconv"
	"errors"
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
        //set the responseHash
        responseHash = &self.Hash
        return nil
    }
    if err != nil && err == rpc.ErrShutdown{
        return err
    }
    return nil
}

func NewRemoteKeeper(addr string, this int) *remoteKeeper{
    //compute hash
    hash := HashBinKey(addr)
    return &remoteKeeper{Addr:addr, This:this,Hash:hash, Connection:nil}
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
	hash          uint64
	lowerBound    uint64
	remoteKeepers []remoteKeeper
	backends      []backend
	errChan       chan error
	replicators   map[int]int
}

//This is tthe place where we send heart beats to remote keepers
func (self *localKeeper) pingNeighbor(){

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
		//This maps indexes of the backends struct list to index
		for {
			time.Sleep(1 * time.Millisecond)
			for p,r := range self.replicators {

				//TODO: HERE IS THE CLIENT IN THE STRUCT: backend_structs_list[0].store.Get
				//TODO: We should mark the lower bound replica/lower bound master on the backends here
				primary := self.backends[p].store
				replica := self.backends[r].store

				var rawLog trib.List
				err := primary.ListGet(LogKey,&rawLog)
				//TODO:Check if replicator is down
				if err!=nil{
					//TODO:In this event we need to promote the replicator to be primary
				}
				for _,cmd := range rawLog.L{
					var n int
					err = execute(replica,cmd)
					if err != nil {
						//TODO:In this condition we need to find a new replicator
					}else{
						//execute on the primary
						err = execute(primary,cmd)
						if err != nil {
							//TODO:In this condition we need to promote the replicator to be primary
						}else{
							log_kv := trib.KV(LogKey, cmd)
							err = primary.ListRemove(log_kv, &n)
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
	}

//This is the function that figures out when backends come up/go down and handles copying of all data from one backend to annother
func (self *localKeeper) backendManager(){

}


//This is the heart beat function we need to call through RPC
func (self *localKeeper) HeartBeat(senderHash uint64, responseHash *uint64) error{
	//TODO: we need to use the senderHash to figure out if we need to change our lower bound.
	//TODO: we then need to send what our lower bound is.
}


func ServeKeeper(kc *trib.KeeperConfig) error {

  //create keeper structs list
  keeper_structs_list := make([]keeper, 0, len(kc.Addrs))
	var this_keeper *keeper
  for i := range kc.Addrs{
    keeper_structs_list = append(keeper_structs_list, keeper{Addr: kc.Addrs[i], This: i, Hash: HashBinKey(kc.Backs[i]), Connection: nil })
		if(i == kc.This){
			this_keeper = keeper_structs_list[i]
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
		this_keeper.Hash,
		this_keeper.Hash,
		keeper_structs_list,
		backend_structs_list,
		errChan,
		make(map[int]int)}

	go keeper.pingNeighbor()
	go keeper.backendManager()
	go keeper.clockManager()
	go keeper.replicationManager()

  if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }

  err := <- errChan

  return err
}

func execute(backend trib.Storage, cmd string) (string,error){
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



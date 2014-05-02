package triblab

import (
	"trib"
	"hash/fnv"
	"sort"
	"strconv"
	"errors"
)

func HashBinKey(word string) uint64{
	hasher := fnv.New64a()
	hasher.Write([]byte("Jq0r6pLVtsXPNkVoliqAyvdZprpwtzPvgQk7WVmX"))
	word2 := "SQA4ZC8m6DhmWhhPhKyN" + word
	chars := make([]byte,3,3)
	hasher.Write([]byte(word2))
	for i := range(word2){
		chars[0] = word2[i]
		chars[2] = word2[len(word2)-i-1]
		chars[1] = chars[1] + chars[0] + chars[2]
		hasher.Write(chars)
	}
	return hasher.Sum64()%100000
}

func NewBinClient(backs []string) trib.BinStorage {
	backends := make([]backend,0,len(backs))
	for i := range backs {
		addr := backs[i]
		hash := HashBinKey(addr)

		store := NewClient(addr)

		backends = append(backends, backend{ addr, hash, store })
	}
	sort.Sort(byHash(backends))
	return &binClient{ backends }
}

type binClient struct{
	backs []backend
}

func (self *binClient) Bin(name string) trib.Storage{
	hash := HashBinKey(name)
	ind := sort.Search(len(self.backs), func(i int) bool{ return self.backs[i].hash >= hash})
	if ind == -1 || ind == len(self.backs) {
		ind = 0
	}
	return NewProxy(name,self.backs[ind].store)
}

type backend struct{
	addr      string
	hash      uint64
	store     trib.Storage
}

type byHash []backend
func (v byHash) Len() int { return len(v) }
func (v byHash) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v byHash) Less(i, j int) bool { return v[i].hash < v[j].hash }

func NewBinStorageProxy(bin string, backs []backend) trib.Storage {
	backends := make([]backend,0,len(backs))
	for _,b := range backs{
		backends = append(backends, backend{
				b.addr,
				b.hash,
				NewProxy(bin, b.store)})
	}

	return &binStorageProxy{
		bin,
		HashBinKey(bin),
		backends,
		nil }
}

func (self *binStorageProxy) checkBackend(back *backend) bool{
	if back == nil{
		return false
	}
	store := back.store
	var result string
	err := store.Get(MasterKeyLB, &result)
	if err != nil{ return false }
	masterlb, err := strconv.ParseUint(result,10,64)
	if err != nil{ return false }

	if masterlb < self.bin_hash && self.bin_hash <= back.hash || back.hash < masterlb && (self.bin_hash <= back.hash || masterlb < self.bin_hash ){
		return true
	}else{
		return false
	}
}

func (self *binStorageProxy) findBackend() *backend{
	if self.checkBackend(self.bin_back){
		return self.bin_back
	}else{
		self.bin_back = nil
	}
	for self.bin_back == nil{
		for _, back := range self.all_backs{
			if self.checkBackend(&back){
				self.bin_back = &back
			}
		}
	}
	return self.bin_back
}

type binStorageProxy struct{
	bin       string
	bin_hash  uint64
	all_backs []backend
	bin_back  *backend
}

func (self *binStorageProxy) Get(key string, value *string) error{
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.Get(key,value)
	}
	return nil
}

func (self *binStorageProxy) Set(kv *trib.KeyValue, succ *bool) error{
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.Set(kv,succ)
	}
	return nil
}

func (self *binStorageProxy) Keys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.Keys(p,list)
	}
	return nil
}

func (self *binStorageProxy) ListGet(key string, list *trib.List) error{
	list.L = make([]string,0)
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.ListGet(key,list)
	}
	return nil
}

func (self *binStorageProxy) ListAppend(kv *trib.KeyValue, succ *bool) error{
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.ListAppend(kv,succ)
	}
	return nil
}

func (self *binStorageProxy) ListRemove(kv *trib.KeyValue, n *int) error{
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.ListRemove(kv,n)
	}
	return nil
}

func (self *binStorageProxy) ListKeys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.ListKeys(p,list)
	}
	return nil
}

func (self *binStorageProxy) Clock(atLeast uint64, ret *uint64) error{
	err  := errors.New("FILLER")
	for err != nil{
		back := self.findBackend()
		err   = back.store.Clock(atLeast,ret)
	}
	return nil
}

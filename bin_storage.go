package triblab

import (
	"trib"
	"hash/fnv"
	"sort"
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

		store := NewLoggingStorage( NewClient(addr) )

		backends = append(backends, backend{ addr, hash, store })
	}
	sort.Sort(byHash(backends))
	return &binClient{ backends }
}

type binClient struct{
	backs []backend
}

func (self *binClient) Bin(name string) trib.Storage{
	//TODO: Need to add code here to ensure we are pointing at the correct "master"
	hash := HashBinKey(name)
	ind := sort.Search(len(self.backs), func(i int) bool{ return self.backs[i].hash >= hash})
	if ind == -1 || ind == len(self.backs) {
		ind = 0
	}
	return NewProxy(name,self.backs[ind].store)
}

type backend struct{
	addr  string
	hash  uint64
	store trib.Storage
}

type byHash []backend
func (v byHash) Len() int { return len(v) }
func (v byHash) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v byHash) Less(i, j int) bool { return v[i].hash < v[j].hash }


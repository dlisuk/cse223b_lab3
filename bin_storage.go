package triblab

import (
	"trib"
	"hash/fnv"
	"encoding/base64"
	"sort"
)

func hashBinKey(word string) string{
	hasher := fnv.New32a()
	return base64.URLEncoding.EncodeToString(hasher.Sum([]byte(word)))
}
func NewBinClient(backs []string) trib.BinStorage {
	backends := make([]backend,0,len(backs))
	for i := range backs {
		addr := backs[i]
		hash := hashBinKey(addr)

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
	hash := hashBinKey(name)
	ind := sort.Search(len(self.backs), func(i int) bool{ return self.backs[i].hash >= hash})
	if ind == -1 || ind == len(self.backs) {
		ind = 0
	}
	return NewProxy(name,self.backs[ind].store)
}

type backend struct{
	addr  string
	hash  string
	store trib.Storage
}

type byHash []backend
func (v byHash) Len() int { return len(v) }
func (v byHash) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v byHash) Less(i, j int) bool { return v[i].hash < v[j].hash }


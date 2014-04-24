package triblab

import (
	"trib"
	"hash/fnv"
	//"encoding/base64"
	"strconv"
	"sort"
)

func hashBinKey(word string) string{
	hasher := fnv.New32a()
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
	return strconv.FormatInt(int64(hasher.Sum32()) % 1000000,10)
}
func NewBinClient(backs []string) trib.BinStorage {
	backends := make([]backend,0,len(backs))
	for i := range backs {
		addr := backs[i]
		print(addr)
		print("-")
		hash := hashBinKey(addr)
		print(hash)
		print("\n")

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
	print(name)
	hash := hashBinKey(name)
	print(":")
	print(hash)
	print(":")
	ind := sort.Search(len(self.backs), func(i int) bool{ return self.backs[i].hash >= hash})
	if ind == -1 || ind == len(self.backs) {
		ind = 0
	}
	print(self.backs[ind].addr)
	print("\n")
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


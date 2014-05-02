package triblab

type keeperByHash []keeper
func (v keeperByHash) Len() int { return len(v) }
func (v keeperByHash) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v keeperByHash) Less(i, j int) bool { return v[i].Hash < v[j].Hash }


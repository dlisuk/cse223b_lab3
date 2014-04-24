package triblab

import(
	"trib"
	"strings"
	"trib/colon"
)

func NewProxy(prefix string, store trib.Storage) trib.Storage{
	return &proxy{ colon.Escape(prefix) + "::", store }
}

type proxy struct{
	prefix string
	store trib.Storage
}

func (self *proxy) makeKey(key string) string{
	return self.prefix + colon.Escape(key)
}
func (self *proxy) extractKey(key string) string{
	return colon.Unescape(strings.TrimPrefix(key, self.prefix))
}

func (self *proxy) escapePattern(p *trib.Pattern) *trib.Pattern{
	prefix := self.makeKey(p.Prefix)
	suffix := colon.Escape(p.Suffix)
	return &trib.Pattern{prefix,suffix}
}

func (self *proxy) Get(key string, value *string) error{
	return self.store.Get(self.makeKey(key),value)
}

func (self *proxy) Set(kv *trib.KeyValue, succ *bool) error{
	key := self.makeKey(kv.Key)
	value := kv.Value
	return self.store.Set(trib.KV(key,value),succ)
}

func (self *proxy) Keys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)

	p_esc := self.escapePattern(p)
	var tmpList trib.List
	err :=  self.store.Keys(p_esc,&tmpList)
	if err != nil { return err }

	for i := range tmpList.L{
		if strings.HasPrefix(tmpList.L[i], self.prefix ) {
			list.L = append(list.L, self.extractKey(tmpList.L[i]))
		}
	}
	return nil
}

func (self *proxy) ListGet(key string, list *trib.List) error{
	list.L = make([]string,0)
	return self.store.ListGet(self.makeKey(key),list)
}

func (self *proxy) ListAppend(kv *trib.KeyValue, succ *bool) error{
	key := self.makeKey(kv.Key)
	value := kv.Value
	return self.store.ListAppend(trib.KV(key,value),succ)
}

func (self *proxy) ListRemove(kv *trib.KeyValue, n *int) error{
	key := self.makeKey(kv.Key)
	value := kv.Value
	return self.store.ListRemove(trib.KV(key,value),n)
}

func (self *proxy) ListKeys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)

	p_esc := self.escapePattern(p)
	var tmpList trib.List
	err :=  self.store.ListKeys(p_esc, &tmpList)
	if err != nil { return err }

	for i := range tmpList.L{
		if strings.HasPrefix(tmpList.L[i], self.prefix ) {
			list.L = append(list.L, self.extractKey(tmpList.L[i]))
		}
	}
	return nil
}

func (self *proxy) Clock(atLeast uint64, ret *uint64) error{
	return self.Clock(atLeast,ret)
}

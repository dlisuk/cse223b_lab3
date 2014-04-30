package triblab

import(
"trib"
"fmt"
"errors"
"strings"
"trib/colon"
)

func NewLoggingStorage( store trib.Storage) trib.Storage{
	return &loggingStorage{ store }
}

type loggingStorage struct{
	store trib.Storage
}

func (self *loggingStorage) issue(cmd string, kv *trib.KeyValue) error{
	var clk uint64
	self.store.Clock(uint64(0),&clk)
	issueString := MakeCmd(clk, cmd, kv)
	issueKV     := trib.KV(LogKey,issueString)
	var succ bool
	self.store.ListAppend(issueKV, &succ)

	return nil
}

func (self *loggingStorage) Get(key string, value *string) error{
	return self.store.Get(key,value)
}

func (self *loggingStorage) Set(kv *trib.KeyValue, succ *bool) error{
	self.issue("SET",kv)
	return nil
}

func (self *loggingStorage) Keys(p *trib.Pattern, list *trib.List) error{
	return self.store.Keys(p,list)
}

func (self *loggingStorage) ListGet(key string, list *trib.List) error{
	return self.store.ListGet(key,list)
}

func (self *loggingStorage) ListAppend(kv *trib.KeyValue, succ *bool) error{
	panic("TODO")
}

func (self *loggingStorage) ListRemove(kv *trib.KeyValue, n *int) error{
	panic("TODO")
}

func (self *loggingStorage) ListKeys(p *trib.Pattern, list *trib.List) error{
	return self.store.ListKeys(p,list)
}

func (self *loggingStorage) Clock(atLeast uint64, ret *uint64) error{
	return self.store.Clock(atLeast,ret)
}

func MakeCmd(clk uint64, cmd string,kv *trib.KeyValue) string{
	return fmt.Sprintf("%v::%s::%s::%s",clk,cmd,colon.Escape(kv.Key),colon.Escape(kv.Value))
}
func ExtractCmd(cmd string) (string, *trib.KeyValue, error){
	fields := strings.Split(cmd, "::")
	if len(fields) != 4 { return nil, nil, errors.New("Insufficient Fields In CMD: " + cmd) }
	kv  := trib.KV(colon.Unescape(fields[2]),colon.Unescape(fields[3]))
	com := fields[1]
	return com, kv, nil
}

package triblab

import(
	"trib"
	"fmt"
	"errors"
	"strings"
	"trib/colon"
	"time"
)

func NewLoggingStorage( store trib.Storage) trib.Storage{
	return &loggingStorage{ store }
}

type loggingStorage struct{
	store trib.Storage
}

func (self *loggingStorage) issue(cmd string, kv *trib.KeyValue, finalSucc *bool) error{
	*finalSucc = false
	var succ bool
	var clk uint64
	self.store.Clock(uint64(0),&clk)
	issueString := MakeCmd(clk, cmd, kv)
	issueKV     := trib.KV(LogKey,issueString)
	err         := self.store.ListAppend(issueKV, &succ)
	if err != nil { return err }
	if !succ      { return errors.New("Failed to append to log") }

	var res trib.List
	for succ == false {
		err := self.store.ListGet(LogKey,&res)
		if err != nil { return err }
		succ = true
		for _,v := range res.L{
			if v == issueString{
				succ = false
			}
		}
		time.Sleep(250)
	}
	*finalSucc = true
	return nil
}

func (self *loggingStorage) Get(key string, value *string) error{
	return self.store.Get(key,value)
}

func (self *loggingStorage) Set(kv *trib.KeyValue, succ *bool) error{
	return self.issue("Storage.Set",kv,succ)
}

func (self *loggingStorage) Keys(p *trib.Pattern, list *trib.List) error{
	return self.store.Keys(p,list)
}

func (self *loggingStorage) ListGet(key string, list *trib.List) error{
	return self.store.ListGet(key,list)
}

func (self *loggingStorage) ListAppend(kv *trib.KeyValue, succ *bool) error{
	return self.issue("Storage.ListAppend",kv,succ)
}

func (self *loggingStorage) ListRemove(kv *trib.KeyValue, n *int) error{
	var succ bool
	return self.issue("Storage.ListRemove",kv,& succ)
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
	if len(fields) != 4 { return "", nil, errors.New("Insufficient Fields In CMD: " + cmd) }
	kv  := trib.KV(colon.Unescape(fields[2]),colon.Unescape(fields[3]))
	com := fields[1]
	return com, kv, nil
}

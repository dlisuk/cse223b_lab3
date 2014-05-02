package triblab

import (
	"trib"
	"net/rpc"
	"net"
	"net/http"
	"strings"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
	return &retryclient{ 3, &client{addr, nil, true}}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	s := rpc.NewServer()
	s.RegisterName("Storage", b.Store)
	listener, err := net.Listen("tcp",b.Addr)

	if err != nil{
		if b.Ready != nil { go func(ch chan<- bool) { ch <- false } (b.Ready) }
		return err
	}

	if b.Ready != nil { go func(ch chan<- bool ) { ch <- true } (b.Ready) }
	return http.Serve(listener, s)
}

type client struct{
	addr string
	connection *rpc.Client
	shutdown bool
}

func (self *client) getConnection() (*rpc.Client, error) {
	c := self.connection
	var err error = nil
	if c == nil {
		c, err = rpc.DialHTTP("tcp", self.addr)
		self.connection = c
		if err != nil && strings.Contains(err.Error(), "connection refused") {
			return c, err
		}
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}


func (self *client) Get(key string, value *string) error{
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("Storage.Get",key,value)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) Set(kv *trib.KeyValue, succ *bool) error{
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("Storage.Set",kv,succ)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) Keys(p *trib.Pattern, list *trib.List) error{
	c, err := self.getConnection()
	list.L = make([]string,0)
	if c != nil {
		err = c.Call("Storage.Keys",p,list)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) ListGet(key string, list *trib.List) error{
	c, err := self.getConnection()
	list.L = make([]string,0)
	if c != nil {
		err = c.Call("Storage.ListGet",key,list)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error{
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("Storage.ListAppend",kv,succ)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) ListRemove(kv *trib.KeyValue, n *int) error{
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("Storage.ListRemove",kv,n)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) ListKeys(p *trib.Pattern, list *trib.List) error{
	c, err := self.getConnection()
	list.L = make([]string,0)
	if c != nil {
		err = c.Call("Storage.ListKeys",p,list)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}

func (self *client) Clock(atLeast uint64, ret *uint64) error{
	c, err := self.getConnection()
	if c != nil {
		err = c.Call("Storage.Clock",atLeast,ret)
	}
	if err != nil && err == rpc.ErrShutdown{
		self.connection = nil
	}
	return err
}


type retryclient struct{
	tries int
	store trib.Storage
}

func (self *retryclient) Get(key string, value *string) error{
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.Get(key,value)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) Set(kv *trib.KeyValue, succ *bool) error{
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.Set(kv,succ)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) Keys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.Keys(p,list)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) ListGet(key string, list *trib.List) error{
	list.L = make([]string,0)
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.ListGet(key,list)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) ListAppend(kv *trib.KeyValue, succ *bool) error{
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.ListAppend(kv,succ)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) ListRemove(kv *trib.KeyValue, n *int) error{
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.ListRemove(kv,n)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) ListKeys(p *trib.Pattern, list *trib.List) error{
	list.L = make([]string,0)
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.ListKeys(p,list)
		if err == nil{
			return err
		}
	}
	return err
}

func (self *retryclient) Clock(atLeast uint64, ret *uint64) error{
	var err error
	for i:=0; i < self.tries; i++{
		err = self.store.Clock(atLeast,ret)
		if err == nil{
			return err
		}
	}
	return err
}

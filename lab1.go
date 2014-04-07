package triblab

import (
	"trib"
	"net/rpc"
	"net/http"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
	connection, err := rpc.DialHTTP("tcp",addr)
	if err != nil{
		panic("Error connecting to rpc server")
	}

	return &client{connection}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	rpc.RegisterName("Lab1", b.Store)
	rpc.HandleHTTP()
	err := http.ListenAndServe(b.Addr, nil)
	return err
}

type client struct{
	connection *rpc.Client
}

func (self *client) Get(key string, value *string) error{
	err := self.connection.Call("Lab1.Get",key,value)
	return err
}

func (self *client) Set(kv *trib.KeyValue, succ *bool) error{
	err := self.connection.Call("Lab1.Set",kv,succ)
	return err
}

func (self *client) Keys(p *trib.Pattern, list *trib.List) error{
	err := self.connection.Call("Lab1.Keys",p,list)
	return err
}

func (self *client) ListGet(key string, list *trib.List) error{
	err := self.connection.Call("Lab1.ListGet",key,list)
	return err
}

func (self *client) ListAppend(kv *trib.KeyValue, succ *bool) error{
	err := self.connection.Call("Lab1.ListAppend",kv,succ)
	return err
}

func (self *client) ListRemove(kv *trib.KeyValue, n *int) error{
	err := self.connection.Call("Lab1.ListRemove",kv,n)
	return err
}

func (self *client) ListKeys(p *trib.Pattern, list *trib.List) error{
	err := self.connection.Call("Lab1.ListKeys",p,list)
	return err
}

func (self *client) Clock(atLeast uint64, ret *uint64) error{
	err := self.connection.Call("Lab1.Clock",atLeast,ret)
	return err
}

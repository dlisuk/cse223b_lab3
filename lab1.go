package triblab

import (
	"trib"
	"net/rpc"
	"net"
)

// Creates an RPC client that connects to addr.
func NewClient(addr string) trib.Storage {
	connection, err := rpc.Dial("tcp",addr)
	if err != nil{
		panic("Error connecting to rpc server")
	}

	return &client{connection}
}

// Serve as a backend based on the given configuration
func ServeBack(b *trib.BackConfig) error {
	s := rpc.NewServer()
	s.RegisterName("Lab1", b.Store)
	listener, err := net.Listen("tcp",b.Addr)
	if err != nil{
		return err
	}
	select{
	case b.Ready <- true:
	default:
	}
	for {
		connection, err  := listener.Accept()
		if err != nil{
			return err
		}
		s.ServeConn(connection)
	}
	return nil
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

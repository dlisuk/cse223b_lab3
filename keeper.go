package triblab

import (
	"trib"
	"time"
	"net/rpc"
)

func ServeKeeper(kc *trib.KeeperConfig) error {
	backs :=  make([]trib.Storage, 0, len(kc.Backs))
	for i := range kc.Backs{
		backs = append(backs,NewClient(kc.Backs[i]))
	}
	if kc.Ready != nil { go func(ch chan<- bool) { ch <- true } (kc.Ready) }
	var highest uint64
	ticker  := time.NewTicker(time.Second)
	errChan := make(chan error)
	seenClocks := make(chan uint64)

	go func(tick <-chan time.Time){
		for {
			_ = <- tick
			go func() {
				for i := range backs {
					go func(back trib.Storage) {
						var ret uint64
						err := back.Clock(highest, &ret)
						if err != nil && err != rpc.ErrShutdown{
							errChan <- err
						}
						seenClocks <- ret
					}(backs[i])
				}
				var maxClock uint64
				for _ = range backs {
					nextClock := <-seenClocks
					if nextClock > maxClock{
						maxClock = nextClock
					}
				}
				if maxClock > highest{
					highest = maxClock
				}else{
					highest = highest + 1
				}
			}()
		}
	}(ticker.C)

	err := <- errChan
	return err
}

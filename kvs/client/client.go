package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	Hosts []*ServerClientConn
	Name  string
	//Response -> Op
	active *sync.Map
	//Response -> Op
	waiting  *sync.Map
	maxBatch int
	opsDone  *atomic.Uint64
}

func (c Client) GetHost(i int) *ServerClientConn {
	return c.Hosts[i]
}

func (c Client) GetName() string {
	return c.Name
}

func (c Client) GetOpsDone() *atomic.Uint64 {
	return c.opsDone
}

func (Client) ShouldBatchRPCs() bool {
	return true
}

const (
	//constants for batching
	MAX_BATCH  = 4096
	FLUSH_FREQ = time.Millisecond
)

// performs one RPC in sync
func (cli Client) doRPC(op *kvs.Op) any {
	client := cli.getShard(op.Key)
	var request any
	var response any
	var rpcStr string

	switch op.Type {
	case kvs.READ:
		request = &kvs.GetRequest{Key: op.Key}
		response = &kvs.GetResponse{}
		rpcStr = "KVService.Get"

		break
	case kvs.WRITE:
		request = &kvs.PutRequest{Key: op.Key, Value: op.Value}
		response = &kvs.Response{}
		rpcStr = "KVService.Put"
		break
	case kvs.COMMIT:
		panic("Client doesn't support COMMIT, use TxnClient")
	case kvs.ABORT:
		panic("Client doesn't support ABORT, use TxnClient")

	}
	err := client.rpcClient.Call(rpcStr, request, response)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	return response
}

func (cli *Client) GetRPC(key string) chan kvs.DataResponse {
	ret := make(chan kvs.DataResponse)
	op := kvs.Op{Key: key, Type: kvs.READ}
	if cli.ShouldBatchRPCs() {
		cli.waiting.Store(ret, op)
	} else {
		go func() {
			v, _ := cli.doRPC(&op).(kvs.DataResponse)
			ret <- v
		}()
	}
	return ret
}

func (cli *Client) PutRPC(key string, value string) chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	op := kvs.Op{Key: key, Value: value, Type: kvs.WRITE}
	if cli.ShouldBatchRPCs() {
		cli.waiting.Store(ret, op)
	} else {
		go func() {
			v, _ := cli.doRPC(&op).(kvs.ResponseInterface)
			ret <- v
		}()
	}
	return ret
}

/* commented out due to errors
// processes any pending ops as a batch
func (client *Client) processBatch() {
	var liveShards []*ServerClientConn
	client.waiting.Range(func(k any, v any) bool {
		vr := v.(kvs.BatchOp)
		sh := client.getShard(vr.Key)
		sh.sendq = append(sh.sendq, vr)
		sh.op2chan.Store(v, k)
		liveShards = append(liveShards, sh)
		if vr.Type == kvs.ABORT || vr.Type == kvs.COMMIT {
			panic("ABORT and COMMIT are not supported with batching")
		}
		return true
	})
	for _, elem := range liveShards {
		request, response := kvs.RequestBatch{Ops: elem.sendq, Src: client.Name}, kvs.ResponseBatch{}
		if err := elem.rpcClient.Call("KVService.Batch", &request, &response); err != nil {
			log.Fatal(err)
		}
		for _, op := range elem.sendq {

			switch op.Type {
			case kvs.READ:
				elem.op2chan.Load(op)
				break
			case kvs.WRITE:
				break
			}
		}
	}
}


func runClient(id int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	//batchSize := 4096
	//ttlFlush := time.Millisecond
	cli := &Client{Name: uuid.New().String(), Hosts: make([]*ServerClientConn, len(addrs))}
	//var wg sync.WaitGroup
	var opsDone atomic.Uint64

	for i, addr := range addrs {
		cli.Hosts[i] = Dial(addr)
	}

	flushOne := func(h *ServerClientConn) {
		if len(h.active) == 0 {
			return
		}
		full := h.active
		h.active, h.spare = h.spare[:0], full
		h.deadline = time.Now().Add(FLUSH_FREQ)
		h.sendq <- full
	}

	flushExpired := func() {
		now := time.Now()
		for _, h := range cli.Hosts {
			if now.After(h.deadline) && len(h.active) > 0 {
				flushOne(h)
			}
		}
	}

	hosts := cli.Hosts
	for !done.Load() {
		for j := 0; j < 4096 && !done.Load(); j++ {
			op := workload.Next(cli)
			key := strconv.FormatUint(op.Key, 10)
			shard := int(hashKey(key) % uint32(len(hosts)))
			h := hosts[shard]

			if op.IsRead {
				h.active = append(h.active, kvs.Op{IsRead: true, Key: key})
			} else {
				h.active = append(h.active, kvs.Op{IsRead: false, Key: key, Value: value})
			}
			if len(h.active) >= cap(h.active) {
				flushOne(h)
			}
		}
		flushExpired()
	}

	for _, h := range hosts {
		flushOne(h)
	}
	for _, h := range hosts {
		close(h.sendq)
	}
	wg.Wait()

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsDone.Load()
}
*/

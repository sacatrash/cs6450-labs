package main

import (
	"fmt"
	"log"
	"net/rpc"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)

type ServerClientConn struct {
	rpcClient *rpc.Client
	Dest      string
	batchOps  []*kvs.Op
	maxBatch  int
}

type Client struct {
	Hosts []*ServerClientConn
	Name  string
}

func shardForKey(key string, n int) int {
	return int(hashKey(key) % uint32(n))
}

func (cli *Client) getShard(key string) *ServerClientConn {
	return cli.Hosts[shardForKey(key, len(cli.Hosts))]
}

func Dial(addr string) *ServerClientConn {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &ServerClientConn{rpcClient: rpcClient, Dest: addr, maxBatch: MAX_BATCH}
}

// performs one RPC in sync
func (cli Client) doRPC(op *kvs.Op) any {
	client := cli.getShard(op.Key)
	var request any
	var response any
	var rpcStr string

	switch op.Type {
	case kvs.READ:
		request = kvs.GetRequest{Key: op.Key}
		response = kvs.GetResponse{}
		rpcStr = "KVService.Get"

		break
	case kvs.WRITE:
		request = kvs.PutRequest{Key: op.Key, Value: op.Value}
		response = kvs.PutResponse{}
		rpcStr = "KVService.Put"
		break
	case kvs.COMMIT:
		panic("Client doesn't support COMMIT, use TxnClient")
	case kvs.ABORT:
		panic("Client doesn't support ABORT, use TxnClient")

	}
	err := client.rpcClient.Call(rpcStr, &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (cli *Client) GetRPC(key string) chan kvs.GetResponse {
	if cli.ShouldBatchRPCs() {

	} else {
		ret := make(chan kvs.GetResponse)
		op := kvs.Op{Key: key, Type: kvs.READ}
		go func() {
			v, _ := cli.doRPC(&op).(kvs.GetResponse)
			ret <- v
		}()
		return ret
	}
}

func (cli *Client) PutRPC(key string, value string) chan kvs.PutResponse {
	if cli.ShouldBatchRPCs() {

	} else {
		ret := make(chan kvs.PutResponse)
		op := kvs.Op{Key: key, Value: value, Type: kvs.WRITE}
		go func() {
			v, _ := cli.doRPC(&op).(kvs.PutResponse)
			ret <- v
		}()
		return ret
	}
}

func (cli *Client) batch(ops []kvs.Op) chan kvs.ResponseBatch {
	request, response := kvs.RequestBatch{Ops: ops, Src: cli.Name, Dest: client.Dest}, kvs.ResponseBatch{}
	if err := client.rpcClient.Call("KVService.Batch", &request, &response); err != nil {
		log.Fatal(err)
	}
	return response.Values
}

/* ##############################*/

func runClient(id int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	//batchSize := 4096
	//ttlFlush := time.Millisecond
	cli := Client{Name: uuid.New().String(), Hosts: make([]*ServerClientConn, len(addrs))}
	//var wg sync.WaitGroup
	var opsDone atomic.Uint64

	for i, addr := range addrs {
		cli.Hosts[i] = Dial(addr)
	}

	//TODO: modify to support manual abort/commit
	for !done.Load() {
		var txops []kvs.Op
		ops := workload.Next()
		op := kvs.Op{}
		for i := 0; i < len(ops); i++ {
			op = ops[i]

			txops = append(txops, op)

			if op.Type == kvs.ABORT || op.Type == kvs.COMMIT {

			}
			if len(txops) >= 3 {
				//if 3 pending actions, commit
				op = kvs.Op{Type: kvs.COMMIT}
				txops = append(txops, op)
			}
		}
		txid := uuid.New().String()
		for {
			ok, n := runTxn3(hosts, txid, txops)
			if ok {
				opsDone.Add(uint64(n))
				break
			}
			txid = uuid.New().String()
			if done.Load() {
				break
			}
		}
	}
	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsDone.Load()
}

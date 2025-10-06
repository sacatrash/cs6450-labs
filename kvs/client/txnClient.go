package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

const (
	//if more than 1, attempts to retry the rpc after a random delay
	RETRY_COUNT    = 0
	RETRY_MAX_TIME = .01
)

type txParticipant map[int]bool

// TxnClient is of interface GenericRpc and ClientTxnRpc, not ClientRpc
type TxnClient struct {
	Client
	TxID kvs.TxID
}

func (c TxnClient) ShouldBatchRPCs() bool {
	return false
}

func (c TxnClient) GetHost(i int) *kvs.ServerClientConn {
	return c.Client.GetHost(i)
}

func (c TxnClient) GetName() string {
	return c.Client.GetName()
}

func (c TxnClient) GetOpsDone() *atomic.Uint64 {
	return c.Client.GetOpsDone()
}

func (c *TxnClient) GetTxnID() kvs.TxID {
	return c.TxID
}

func (c *TxnClient) SetTxnID(newId kvs.TxID) {
	c.TxID = newId
}

func (c *TxnClient) InvalidateTxnID() {
	c.TxID.Invalidate()
}

// performs one RPC in sync
// if RPC is commit/abort/begin, performs across all shards
// and returns an empty response unless there was an error,
// in which case returns the last error response
func (cli *TxnClient) DoRPC(op *kvs.Op) any {
	TxnID := cli.GetTxnID()
	if TxnID == "" {
		return &kvs.Response{Error: kvs.ERROR_BAD_TXID}
	}
	var request any
	var response any
	var rpcStr string
	switch op.Type {
	case kvs.READ:
		tmp := &kvs.TxGetRequest{Tx: TxnID}
		tmp.Key = op.Key
		request = tmp
		response = &kvs.GetResponse{}
		rpcStr = "KVService.TxGet"
	case kvs.WRITE:
		tmp := &kvs.TxPutRequest{Tx: TxnID}
		tmp.Key = op.Key
		tmp.Value = op.Value
		request = tmp
		response = &kvs.Response{}
		rpcStr = "KVService.TxPut"
	case kvs.COMMIT:
		request = &kvs.TxRequest{Tx: TxnID}
		response = &kvs.Response{}
		rpcStr = "KVService.TxCommit"
	case kvs.ABORT:
		request = &kvs.TxRequest{Tx: TxnID}
		response = &kvs.Response{}
		rpcStr = "KVService.TxAbort"
	case kvs.BEGIN:
		request = &kvs.TxRequest{Tx: TxnID}
		response = &kvs.Response{}
		rpcStr = "KVService.TxBegin"
	}
	var err error
	//fmt.Printf("\n%s %s\n", TxnID, rpcStr)
	if op.Type == kvs.READ || op.Type == kvs.WRITE {
		for b := RETRY_COUNT; b >= 0; b-- {
			client := cli.Client.getShard(op.Key)
			err = client.RpcClient.Call(rpcStr, request, response)
			if err == nil {
				e := response.(kvs.ResponseInterface).GetError()
				if e == kvs.ERROR_SERVER_ABT || !(e == kvs.ERROR_S_LOCK_FAIL || e == kvs.ERROR_X_LOCK_FAIL) {
					break
				} else {
					cli.opsRetried.Add(1)
					time.Sleep(time.Duration(rand.Float64() * RETRY_MAX_TIME))
				}
			}
		}

	} else {
		for i := range cli.Client.Hosts {
			var tmpResponse = &kvs.Response{}
			err = cli.Client.GetHost(i).RpcClient.Call(rpcStr, request, tmpResponse)
			if !response.(kvs.ResponseInterface).IsOk() {
				response = tmpResponse
			}
		}

	}
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	return response
}

func (cli TxnClient) GetRPC(key string) chan kvs.DataResponse {
	ret := make(chan kvs.DataResponse)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.READ, Key: key})
		if v.(kvs.DataResponse).IsOk() {
			ret <- v.(kvs.DataResponse)
		} else {
			tmp := &kvs.GetResponse{}
			tmp.Response.Error = v.(*kvs.GetResponse).Error
			ret <- tmp
		}
	}()
	return ret
}

func (cli *TxnClient) PutRPC(key string, val string) chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.WRITE, Key: key, Value: val})
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (cli *TxnClient) AbortTxnRPC() chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.ABORT})
		ret <- v.(kvs.ResponseInterface)
		cli.TxID.Invalidate()
	}()
	return ret
}

func (cli *TxnClient) CommitTxnRPC() chan kvs.ResponseInterface {
	//once commit is called we cannot abort
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.COMMIT})
		ret <- v.(kvs.ResponseInterface)
		cli.TxID.Invalidate()
	}()
	return ret
}

func (cli *TxnClient) BeginTxnRPC(Tx kvs.TxID) chan kvs.ResponseInterface {
	cli.TxID = Tx
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.BEGIN})
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func RunTxnClient(id int, cli TxnClient /*hosts []string,*/, done *atomic.Bool, workload kvs.DefaultWorkload, resultsCh chan<- uint64, retriesCh chan<- uint64) {

	/*cli := TxnClient{}
	cli.Name = uuid.New().String()
	cli.Hosts = make([]*kvs.ServerClientConn, len(hosts))
	cli.opsDone = &atomic.Uint64{}

	for i, addr := range hosts {
		cli.Hosts[i] = Dial(addr)
	}*/

	for !done.Load() {
		//initialize txn
		init := <-cli.BeginTxnRPC(kvs.GetNew(cli.GetName() + strconv.FormatInt(int64(id), 10)))

		if cli.TxID.IsValid() {
			if !init.IsOk() {
				<-cli.AbortTxnRPC()
				continue
			}
			res := workload.Next(&cli)

			//on failure, abort current txn
			if !res {
				<-cli.AbortTxnRPC()
			} else {
				<-cli.CommitTxnRPC()
				cli.GetOpsDone().Add(1)
			}
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- cli.GetOpsDone().Load()
	retriesCh <- cli.opsRetried.Load()
}

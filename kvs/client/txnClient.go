package main

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type txParticipant map[int]bool

// TxnClient is of interface GenericRpc and ClientTxnRpc, not ClientRpc
type TxnClient struct {
	Client *Client
	TxID   kvs.TxID
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

func (c TxnClient) GetTxnID() kvs.TxID {
	return c.TxID
}

func (c TxnClient) SetTxnID(newId kvs.TxID) {
	c.TxID = newId
}

func (c TxnClient) InvalidateTxnID() {
	c.TxID.Invalidate()
}

func (cli TxnClient) getShard(key string) *kvs.ServerClientConn {
	return cli.Hosts[kvs.ShardForKey(key, len(cli.Hosts))]
}

// performs one RPC in sync
// if RPC is commit/abort/begin, performs across all shards
// and returns an empty response unless there was an error,
// in which case returns the last error response
func (c TxnClient) DoRPC(op *kvs.Op) any {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
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
	fmt.Printf("\n%s %s\n", TxnID, rpcStr)
	if op.Type == kvs.READ || op.Type == kvs.WRITE {
		client := cli.Client.getShard(op.Key)
		err = client.RpcClient.Call(rpcStr, request, response)

	} else {
		for i := range cli.Client.Hosts {
			var tmpResponse = &kvs.Response{}
			err = cli.Client.GetHost(i).RpcClient.Call(rpcStr, request, tmpResponse)
			if !response.(*kvs.Response).IsOk() {
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

func (c TxnClient) GetRPC(key string) chan kvs.DataResponse {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
	ret := make(chan kvs.DataResponse)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.READ, Key: key})
		if v.(*kvs.Response).IsOk() {
			ret <- v.(kvs.DataResponse)
		} else {
			tmp := &kvs.GetResponse{}
			tmp.Response.Error = v.(*kvs.Response).Error
			ret <- tmp
		}
	}()
	return ret
}

func (c TxnClient) PutRPC(key string, val string) chan kvs.ResponseInterface {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.WRITE, Key: key, Value: val})
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (c TxnClient) AbortTxnRPC() chan kvs.ResponseInterface {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.ABORT})
		ret <- v.(kvs.ResponseInterface)
		cli.InvalidateTxnID()
	}()
	return ret
}

func (c TxnClient) CommitTxnRPC() chan kvs.ResponseInterface {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
	//once commit is called we cannot abort
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.COMMIT})
		ret <- v.(kvs.ResponseInterface)
		cli.InvalidateTxnID()
	}()
	return ret
}

func (c TxnClient) BeginTxnRPC(Tx kvs.TxID) chan kvs.ResponseInterface {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)
	cli.SetTxnID(Tx)
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.DoRPC(&kvs.Op{Type: kvs.BEGIN})
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (c *TxnClient) RunClient(id int, done *atomic.Bool, workload kvs.TxnWorkload, resultsCh chan<- uint64) {
	var cli kvs.ClientTxnRpc
	cli = kvs.ClientTxnRpc(c)

	for !done.Load() {
		//initialize txn
		init := <-cli.BeginTxnRPC(kvs.GetNew(cli.GetName() + strconv.FormatInt(int64(id), 10)))

		if cli.GetTxnID().IsValid() {
			if !init.IsOk() {
				cli.AbortTxnRPC()
				continue
			}
			res := workload.Next(cli)

			//on failure, abort current txn
			if !res {
				cli.AbortTxnRPC()
			} else {
				cli.GetOpsDone().Add(1)
			}
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- cli.GetOpsDone().Load()
}

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
	Client
}

func (c TxnClient) GetHost(i int) *ServerClientConn {
	return c.Client.GetHost(i)
}

func (c TxnClient) GetName() string {
	return c.Client.GetName()
}

func (c TxnClient) GetOpsDone() *atomic.Uint64 {
	return c.Client.GetOpsDone()
}

// performs one RPC in sync
func (cli TxnClient) doRPC(op *kvs.Op, TxnID kvs.TxID) any {
	client := cli.getShard(op.Key)
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
	err := client.rpcClient.Call(rpcStr, request, response)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	return response
}

func (cli TxnClient) GetTxnRPC(key string, TxId kvs.TxID) chan kvs.DataResponse {
	ret := make(chan kvs.DataResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.READ, Key: key}, TxId)
		ret <- v.(kvs.DataResponse)
	}()
	return ret
}

func (cli TxnClient) PutTxnRPC(key string, val string, TxId kvs.TxID) chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.WRITE, Key: key, Value: val}, TxId)
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (cli TxnClient) AbortTxnRPC(TxId kvs.TxID) chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.ABORT}, TxId)
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (cli TxnClient) CommitTxnRPC(TxId kvs.TxID) chan kvs.ResponseInterface {
	//once commit is called we cannot abort
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.COMMIT}, TxId)
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (cli TxnClient) BeginTxnRPC(TxId kvs.TxID) chan kvs.ResponseInterface {
	ret := make(chan kvs.ResponseInterface)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.BEGIN}, TxId)
		ret <- v.(kvs.ResponseInterface)
	}()
	return ret
}

func (cli *TxnClient) runTxnClient(id int, done *atomic.Bool, workload kvs.TxnWorkload, resultsCh chan<- uint64) {
	//batchSize := 4096
	//ttlFlush := time.Millisecond

	for !done.Load() {
		//initialize txn
		TxnID := kvs.GetNew(cli.Name + strconv.FormatInt(int64(id), 10))
		init := <-cli.BeginTxnRPC(TxnID)

		if !init.IsOk() {
			cli.AbortTxnRPC(TxnID)
			continue
		}
		res := workload.Next(cli, TxnID)

		//on failure, abort current txn
		if !res {
			cli.AbortTxnRPC(TxnID)
		} else {
			cli.GetOpsDone().Add(1)
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- cli.GetOpsDone().Load()
}

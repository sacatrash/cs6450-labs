package main

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)

type txParticipant map[int]bool

// TxnClient is of interface GenericRpc and ClientTxnRpc, not ClientRpc
type TxnClient struct {
	Client
	TxnID kvs.TxID
}

// performs one RPC in sync
func (cli TxnClient) doRPC(op *kvs.Op) any {
	client := cli.getShard(op.Key)
	var request any
	var response any
	var rpcStr string

	switch op.Type {
	case kvs.READ:
		tmp := kvs.TxGetRequest{Tx: cli.TxnID}
		tmp.Key = op.Key
		request = tmp
		response = kvs.TxGetResponse{}
		rpcStr = "KVService.TxGet"
	case kvs.WRITE:
		tmp := kvs.TxPutRequest{Tx: cli.TxnID}
		tmp.Key = op.Key
		tmp.Value = op.Value
		response = kvs.PutResponse{}
		rpcStr = "KVService.TxPut"
	case kvs.COMMIT:
		request = kvs.TxCommitRequest{Tx: cli.TxnID}
		response = kvs.TxCommitResponse{}
		rpcStr = "KVService.TxCommit"
	case kvs.ABORT:
		request = kvs.TxCommitRequest{Tx: cli.TxnID}
		response = kvs.TxCommitResponse{}
		rpcStr = "KVService.TxAbort"
	case kvs.BEGIN:
		request = kvs.TxBeginRequest{Tx: cli.TxnID}
		response = kvs.TxBeginResponse{}
		rpcStr = "KVService.TxBegin"

	}
	err := client.rpcClient.Call(rpcStr, &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (cli TxnClient) GetTxnRPC(key string) chan kvs.TxGetResponse {
	ret := make(chan kvs.TxGetResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.WRITE, Key: key})
		ret <- v.(kvs.TxGetResponse)
	}()
	return ret
}

func (cli TxnClient) PutTxnRPC(key string, val string) chan kvs.TxPutResponse {
	ret := make(chan kvs.TxPutResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.WRITE, Key: key, Value: val})
		ret <- v.(kvs.TxPutResponse)
	}()
	return ret
}

func (cli TxnClient) AbortTxnRPC() chan kvs.TxAbortResponse {
	ret := make(chan kvs.TxAbortResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.ABORT})
		ret <- v.(kvs.TxAbortResponse)
	}()
	return ret
}

func (cli TxnClient) CommitTxnRPC() chan kvs.TxCommitResponse {
	//once commit is called we cannot abort
	ret := make(chan kvs.TxCommitResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.ABORT})
		ret <- v.(kvs.TxCommitResponse)
	}()
	return ret
}

func (cli TxnClient) BeginTxnRPC() chan kvs.TxBeginResponse {
	cli.TxnID.SetNew(cli.Name + uuid.New().String())
	ret := make(chan kvs.TxBeginResponse)
	go func() {
		v := cli.doRPC(&kvs.Op{Type: kvs.BEGIN})
		ret <- v.(kvs.TxBeginResponse)
	}()
	return ret
}

func runTxnClient(id int, addrs []string, done *atomic.Bool, workload kvs.TxnWorkload, resultsCh chan<- uint64) {
	//batchSize := 4096
	//ttlFlush := time.Millisecond
	cli := &TxnClient{}
	cli.Name = uuid.New().String()
	cli.Hosts = make([]*ServerClientConn, len(addrs))
	//var wg sync.WaitGroup
	var opsDone atomic.Uint64

	for i, addr := range addrs {
		cli.Hosts[i] = Dial(addr)
	}

	for !done.Load() {
		//initialize txn

		init := <-cli.BeginTxnRPC()

		if !init.IsOk() {
			cli.AbortTxnRPC()
			continue
		}

		res := workload.Next(cli)

		//on failure, abort current txn
		if !res {
			cli.AbortTxnRPC()
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsDone.Load()
}

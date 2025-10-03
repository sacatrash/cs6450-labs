package main

import (
	"log"
	"net/rpc"
	"sort"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type txParticipant map[int]bool

type TxnClient struct {
	Client
	TxnID int
}

func (TxnClient) ShouldBatchRPCs() bool {
	return false
}

// performs one RPC in sync
func (cli TxnClient) doRPC(op *kvs.Op) any {
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
		request = kvs.TxCommitRequest{}
		response = kvs.TxCommitResponse{}
		rpcStr = "KVService.Put"
	case kvs.ABORT:
		panic("Client doesn't support ABORT, use TxnClient")

	}
	err := client.rpcClient.Call(rpcStr, &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func TxPutRPC(rc *rpc.Client, txid string, key, value string) bool {
	//request := txPutRequest{Tx: TxID(txid), Key:key, Value: value}
	request := kvs.TxPutRequest{Tx: kvs.TxID(txid)}
	request.Key = key
	request.Value = value
	//var response txPutResponse
	var response kvs.TxPutResponse

	if err := rc.Call("KVService.TxPut", &request, &response); err != nil {
		return false
	}
	return response.Ok
}

// 2PC train of thought, when this is called, the client will ask each. server to commit
// if it fails, client can send txabortrpc - AKA 2 phase commit
func TxCommitRPC(rc *rpc.Client, txid string, lead bool) bool {
	//request := txCommitRequest{Tx: TxID(txid), Lead: lead}
	request := kvs.TxCommitRequest{Tx: kvs.TxID(txid), Lead: lead}
	//var response txCommitResponse
	var response kvs.TxCommitResponse
	if err := rc.Call("KVService.TxCommit", &request, &response); err != nil {
		return false
	}
	return response.Ok
}

func TxAbortRPC(rc *rpc.Client, txid string) bool {
	//request := txAbortRequest{Tx: TxID(txid)}
	request := kvs.TxAbortRequest{Tx: kvs.TxID(txid)}
	//var response txAbortResponse
	var response kvs.TxAbortResponse
	if err := rc.Call("KVService.TxAbort", &request, &response); err != nil {
		return false
	}
	return true
}

func TxPrepareRPC(rc *rpc.Client, txid string, items []kvs.TxLockItem) bool {
	request := kvs.TxPrepareRequest{Tx: kvs.TxID(txid), Items: items}
	var response kvs.TxPrepareResponse
	if err := rc.Call("KVService.TxPrepare", &request, &response); err != nil {
		return false
	}
	return response.Ok
}

func (cli TxnClient) GetTxnRPC(key string) chan kvs.TxGetResponse {

}

func (cli TxnClient) PutTxnRPC(key string, val string) chan kvs.TxPutResponse {

}

func (cli TxnClient) AbortTxnRPC() chan kvs.TxAbortResponse {

}

func (cli TxnClient) CommitTxnRPC(key string) chan kvs.TxCommitResponse {

}

func (Client) runTxn3(hosts []*perHost, txid string, ops []kvs.Op) (bool, int) {
	//we need to prepare the build , sort, txprepare for shard etc
	modes := strongestModes(ops)
	type item struct {
		shard int
		key   string
		mode  kvs.LockMode
	}
	var items []item
	for k, md := range modes {
		items = append(items, item{
			shard: shardForKey(k, len(hosts)),
			key:   k,
			mode:  md,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].shard != items[j].shard {
			return items[i].shard < items[j].shard
		}
		return items[i].key < items[j].key
	})
	// we need to group per shard
	type group struct {
		shard int
		list  []kvs.TxLockItem
	}
	var groups []group
	for _, it := range items {
		if len(groups) == 0 || groups[len(groups)-1].shard != it.shard {
			groups = append(groups, group{shard: it.shard})
		}
		groups[len(groups)-1].list = append(
			groups[len(groups)-1].list,
			kvs.TxLockItem{
				Key:  it.key,
				Mode: it.mode,
			},
		)
	}
	participants := txParticipant{}
	for _, g := range groups {
		if !TxPrepareRPC(hosts[g.shard].c.rpcClient, txid, g.list) {
			for p := range participants {
				_ = TxAbortRPC(hosts[p].c.rpcClient, txid)
			}
			return false, 0
		}
		participants[g.shard] = true
	}
	for _, op := range ops {
		sh := shardForKey(op.Key, len(hosts))
		if op.IsRead() {
			if _, ok := TxGetRPC(hosts[sh].c.rpcClient, txid, op.Key); !ok {
				for p := range participants {
					_ = TxAbortRPC(hosts[p].c.rpcClient, txid)
				}
				return false, 0
			}
		} else if op.IsWrite() {
			if !TxPutRPC(hosts[sh].c.rpcClient, txid, op.Key, op.Value) {
				for p := range participants {
					_ = TxAbortRPC(hosts[p].c.rpcClient, txid)
				}
				return false, 0
			}
		}

	}
	//2PC commit for touched shards
	first := true
	okAll := true
	for p := range participants {
		ok := TxCommitRPC(hosts[p].c.rpcClient, txid, first)
		if !ok {
			okAll = false
		}
		first = false
	}
	if !okAll {
		for p := range participants {
			_ = TxAbortRPC(hosts[p].c.rpcClient, txid)
		}
		return false, 0
	}
	return true, len(ops)
}

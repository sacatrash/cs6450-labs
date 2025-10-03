package main

import (
	"log"
	"sort"

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

	//if txnID invalid, begin new txn
	if !cli.TxnID.Valid {

	}

	switch op.Type {
	case kvs.READ:
		tmp := kvs.TxGetRequest{Tx: cli.TxnID}
		tmp.Key = op.Key
		request = tmp
		response = kvs.TxGetResponse{}
		rpcStr = "KVService.TxGet"

		break
	case kvs.WRITE:
		tmp := kvs.TxPutRequest{Tx: cli.TxnID}
		tmp.Key = op.Key
		tmp.Value = op.Value
		response = kvs.PutResponse{}
		rpcStr = "KVService.TxPut"
		break
	case kvs.COMMIT:
		request = kvs.TxCommitRequest{Tx: cli.TxnID}
		response = kvs.TxCommitResponse{}
		rpcStr = "KVService.TxCommit"
	case kvs.ABORT:
		request = kvs.TxCommitRequest{Tx: cli.TxnID}
		response = kvs.TxCommitResponse{}
		rpcStr = "KVService.TxAbort"

	}
	err := client.rpcClient.Call(rpcStr, &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
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

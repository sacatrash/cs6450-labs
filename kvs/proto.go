package kvs

import "sync"

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

type Content struct {
	sync.Mutex
	Order int
	Value string
}


func (*Content c) setContent(newValue string) {
	c.Value = newValue
}
/*
type LockRequest struct {
	locks []*sync.Mutex
	ret   chan int
}
*/

/*NEW WORKLOAD TRANSACTION RPC (REMOTE PROCEDURE CALLS) REQUEST AND RESPONSE STRUCTS */

type TxID string

type Transaction struct {
	//req Request
	Key string
	op Op
	time Time
}

type  TxGetResp struct{
    ok bool
    Value string
}

type TxGetReq struct{
    Tx TxID
    Key string
}

type TxCommitReq struct{
    Tx TxID
    Lead bool
}

type TxCommitResp struct{
    Ok bool
}

type TxPutReq struct{
    Tx TxID
    Key string
    Value string
}

type TxPutResp struct{
    Ok bool
}

type TxAbortReq struct{
    Tx TxID
}

type TxAbortResp struct{}

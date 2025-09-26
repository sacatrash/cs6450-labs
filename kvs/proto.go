package kvs

import "sync"

type Op struct {
	Key string
	Value string
	IsRead bool
}

type Content struct {
	sync.Mutex
	Order int
	Value string
}

func (*Content c) setContent(newValue string) {
	c.Value = newValue
}

type Request struct {

}

type Response struct {
	Ok bool
}

type PutRequest struct {
	Request
	Key string
	Value string
}

type PutResponse struct {
	Response
}

type GetRequest struct {
	Request
	Key string
}

type GetResponse struct {
	Response
	Value string
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
	Response
    ok bool
    Value string
}

type TxGetReq struct{
	Request
    Tx TxID
    Key string
}

type TxCommitReq struct{
	Request
    Tx TxID
    Lead bool
}

type TxCommitResp struct{
	Response
}

type TxPutReq struct{
	Request
    Tx TxID
    Key string
    Value string
}

type TxPutResp struct{
	Response
}

type TxAbortReq struct{
	Request
    Tx TxID
}

type TxAbortResp struct{
	Response
}

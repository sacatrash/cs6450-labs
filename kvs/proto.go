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

//PRE LOCK
type lockMode string
const (
	lock_S lockMode = "S"
	lock_X lockMode = "X"
)

type txLockItem struct {
	Key string
	Mode lockMode
}
//these prepares are to preacquire s/x locks in a single ordered way
type txPrepareResponse struct{
	Response
}

type txPrepareRequest struct{
	Request
	Tx TxID
	Items []txLockItem
}
/*
type Transaction struct {
	//req Request
	Key string
	op Op
	time Time
}
*/
type  txGetResponse struct{
	Response
    //ok bool
    Value string
}

type txGetRequest struct{
	Request
    Tx TxID
    Key string
}

type txCommitRequest struct{
	Request
    Tx TxID
    Lead bool
}

type txCommitResponse struct{
	Response
}

type txPutRequest struct{
	Request
    Tx TxID
    Key string
    Value string
}

type txPutResponse struct{
	Response
}

type txAbortRequest struct{
	Request
    Tx TxID
}

type txAbortResponse struct{
	Response
}

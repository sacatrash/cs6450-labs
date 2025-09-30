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

func (c *Content) setContent(newValue string) {
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

type TxLockItem struct {
	Key string
	Mode lockMode
}
//these prepares are to preacquire s/x locks in a single ordered way
type TxPrepareResponse struct{
	Response
}

type TxPrepareRequest struct{
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
type TxGetResponse struct{
	Response
    //ok bool
    Value string
}

type TxGetRequest struct{
	Request
    Tx TxID
    Key string
}

type TxCommitRequest struct{
	Request
    Tx TxID
    Lead bool
}

type TxCommitResponse struct{
	Response
}

type TxPutRequest struct{
	Request
    Tx TxID
    Key string
    Value string
}

type TxPutResponse struct{
	Response
}

type TxAbortRequest struct{
	Request
    Tx TxID
}

type TxAbortResponse struct{
	Response
}

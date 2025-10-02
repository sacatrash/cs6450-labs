package kvs

import "sync"

const (
	READ   = 0
	WRITE  = 1
	COMMIT = 2
	ABORT  = 3
)

type Op struct {
	Key   string
	Value string
	Type  int //READ, WRITE, COMMIT, ABORT
}

func (o *Op) IsRead() bool {
	return o.Type == READ
}

func (o *Op) IsWrite() bool {
	return o.Type == WRITE
}

func (o *Op) IsCommit() bool {
	return o.Type == COMMIT
}

func (o *Op) IsAbort() bool {
	return o.Type == ABORT
}

type Content struct {
	sync.Mutex
	Order int
	Value string
}

func (c *Content) SetContent(newValue string) {
	c.Value = newValue
}

type Request struct {
}

type Response struct {
	Ok bool
}

type PutRequest struct {
	Request
	Key   string
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

// PRE LOCK
type LockMode string

const (
	Lock_S LockMode = "S"
	Lock_X LockMode = "X"
)

type TxLockItem struct {
	Key  string
	Mode LockMode
}

// these prepares are to preacquire s/x locks in a single ordered way
type TxPrepareResponse struct {
	Response
}

type TxPrepareRequest struct {
	Request
	Tx    TxID
	Items []TxLockItem
}

/*
	type Transaction struct {
		//req Request
		Key string
		op Op
		time Time
	}
*/
type TxGetResponse struct {
	GetResponse
	//ok bool
}

type TxGetRequest struct {
	GetRequest
	Tx TxID
}

type TxCommitRequest struct {
	Request
	Tx   TxID
	Lead bool
}

type TxCommitResponse struct {
	Response
}

type TxPutRequest struct {
	PutRequest
	Tx TxID
}

type TxPutResponse struct {
	PutResponse
}

type TxAbortRequest struct {
	Request
	Tx TxID
}

type TxAbortResponse struct {
	Response
}

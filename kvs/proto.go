package kvs

import (
	"sync"
)

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

type ResponseInterface interface {
	IsOk() bool
}

type Response struct {
	Error error
}

func (r Response) IsOk() bool {
	return r.Error == nil
}

type DataResponse interface {
	Get() string
}

type PutRequest struct {
	Request
	Key   string
	Value string
}

type PutResponse struct {
	Response
}

func (r PutResponse) IsOk() bool {
	return r.Response.IsOk()
}

type GetRequest struct {
	Request
	Key string
}

type GetResponse struct {
	Response
	Value string
}

func (r GetResponse) IsOk() bool {
	return r.Response.IsOk()
}

func (r GetResponse) Get() string {
	return r.Value
}

/*
type LockRequest struct {
	locks []*sync.Mutex
	ret   chan int
}
*/

/*NEW WORKLOAD TRANSACTION RPC (REMOTE PROCEDURE CALLS) REQUEST AND RESPONSE STRUCTS */

type TxID struct {
	string
	Valid bool
}

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

func (r TxGetResponse) IsOk() bool {
	return r.Response.IsOk()
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

func (r TxCommitResponse) IsOk() bool {
	return r.Response.IsOk()
}

type TxPutRequest struct {
	PutRequest
	Tx TxID
}

type TxPutResponse struct {
	PutResponse
}

func (r TxPutResponse) IsOk() bool {
	return r.Response.IsOk()
}

type TxAbortRequest struct {
	Request
	Tx TxID
}

type TxAbortResponse struct {
	Response
}

func (r TxAbortResponse) IsOk() bool {
	return r.Response.IsOk()
}

// generic interface to handle the RPC of an Op
type GenericRpc interface {
	doRPC(op *Op) any
}

//client related types

// Interfaces define the types of RPCs which clients can send
type ClientRpc interface {
	GetRPC(key string) chan GetResponse
	PutRPC(key string, val string) chan PutResponse
	ShouldBatchRPCs() bool //if true, batches RPCs when Get/Put called instead of calling outright
}

type ClientTxnRpc interface {
	GetTxnRPC(key string) chan TxGetResponse
	PutTxnRPC(key string, val string) chan TxPutResponse
	AbortTxnRPC() chan TxAbortResponse
	CommitTxnRPC() chan TxCommitResponse
}

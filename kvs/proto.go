package kvs

import (
	"hash/fnv"
	"sync"
)

const (
	READ   = 0
	WRITE  = 1
	COMMIT = 2
	ABORT  = 3
	BEGIN  = 4
)

const (
	Lock_S  LockMode = "S"
	Lock_X  LockMode = "X"
	LOCK_NA LockMode = "NA"

	ERROR_S_LOCK_FAIL   ResponseError = "Could not acquire read lock"
	ERROR_X_LOCK_FAIL   ResponseError = "Could not acquire write lock"
	ERROR_BAD_TXID      ResponseError = "Incorrect TxID"
	ERROR_NOT_PROCESSED ResponseError = "RPC pending processing in batch."
	ERROR_OTHER         ResponseError = "Unknown error."
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

func HashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func ShardForKey(key string, n int) int {
	return int(HashKey(key) % uint32(n))
}

func StrongestModes(ops []Op) map[string]LockMode {
	m := map[string]LockMode{}
	for _, op := range ops {
		if op.IsRead() {
			if _, ok := m[op.Key]; !ok {
				m[op.Key] = Lock_S
			}
		} else if op.IsWrite() {
			m[op.Key] = Lock_X
		}
	}
	return m
}

type Content struct {
	sync.Mutex
	Key   string
	Value string
}

type ResponseError string

func (e ResponseError) Error() string {
	return string(e)
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

type TxID string

func (t TxID) IsValid() bool {
	return t != ""
}

func (t TxID) Invalidate() {
	t = ""
}

func (t TxID) SetNew(id string) {
	t = TxID(id)
}

// PRE LOCK
type LockMode string

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

func (r TxBeginResponse) IsOk() bool {
	return r.Response.IsOk()
}

type TxBeginRequest struct {
	Request
	Tx TxID
}

type TxBeginResponse struct {
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
	BeginTxnRPC() chan TxBeginResponse
}

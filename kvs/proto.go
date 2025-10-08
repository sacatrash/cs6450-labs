package kvs

import (
	"hash/fnv"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
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
	ERROR_SERVER_ABT    ResponseError = "Server aborted transaction."
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

func (c *Content) SetContent(newValue string) {
	c.Value = newValue
}

type Request struct {
}

type ResponseInterface interface {
	IsOk() bool
	GetError() ResponseError
}

type Response struct {
	Error ResponseError
}

func (r Response) IsOk() bool {
	return r.Error == ""
}

func (r Response) GetError() ResponseError {
	return r.Error
}

type DataResponse interface {
	Get() string
	IsOk() bool
}

type PutRequest struct {
	Request
	Key   string
	Value string
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

func (r GetResponse) GetError() ResponseError {
	return r.Response.GetError()
}

func (r GetResponse) Get() string {
	return r.Value
}

/*NEW WORKLOAD TRANSACTION RPC (REMOTE PROCEDURE CALLS) REQUEST AND RESPONSE STRUCTS */

type TxID string

func (t TxID) IsValid() bool {
	return t != ""
}

func (t *TxID) Invalidate() {
	*t = ""
}

func GetNew(id string) TxID {
	return TxID(id)
}

// PRE LOCK
type LockMode string

type TxGenericRequest interface {
	GetTxID() TxID
}

type TxGetRequest struct {
	GetRequest
	Tx TxID
}

func (t TxGetRequest) GetTxID() TxID {
	return t.Tx
}

type TxPutRequest struct {
	PutRequest
	Tx TxID
}

func (t TxPutRequest) GetTxID() TxID {
	return t.Tx
}

type TxRequest struct {
	Request
	Tx   TxID
	Lead bool
}

func (t TxRequest) GetTxID() TxID {
	return t.Tx
}

// generic interface to handle the RPC of an Op
type GenericRpc interface {
	DoRPC(op *Op) any
}

// client related types
type ServerClientConn struct {
	RpcClient *rpc.Client
	Dest      string
	deadline  time.Time
	sendq     []BatchOp
	//op -> response chan
	op2chan *sync.Map
}

// Interfaces define the types of RPCs which clients can send
type ClientRpc interface {
	GenericRpc
	GetRPC(key string) chan DataResponse
	PutRPC(key string, val string) chan ResponseInterface
	ShouldBatchRPCs() bool //if true, batches RPCs when Get/Put called instead of calling outright
	GetHost(i int) *ServerClientConn
	GetName() string
	GetOpsDone() *atomic.Uint64
}

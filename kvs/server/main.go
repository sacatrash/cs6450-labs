package main

import (
	//"hash/fnv"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type keyLock struct {
	key string
	//readers map[*txState]any
	readers     sync.Map
	readerCount *atomic.Uint32
	writer      *txState
}

type txState struct {
	//if true, forces this commit to abort on the next rpc
	abort bool
	//writes map[string]string
	writes sync.Map
	//s_held map[string]*keyLock
	s_held sync.Map
	//x_held map[string]*keyLock
	x_held sync.Map
	//0=ignore (break serializability); 1=abort write; 2=abort all reads; 3=snapshot (currently breaks serializability)
	readWriteLockStrat int

	//if readWriteLockStrat == 3, this holds a copy of the keys upon Begin
	//do not use to store modified values mid-txn- use writes field instead
	readState sync.Map
}

// use the KVService mutex when we need to add/remove keys
// use the mutexMap mutex for getting/setting existing keys
type KVService struct {
	//sync.Mutex
	//mp        map[string]*atomic.Value
	mu        sync.Mutex //mutex used during lock acquisition/release
	mp        sync.Map   //the data being stored map[string]*Content
	stats     Stats
	prevStats Stats
	lastPrint time.Time
	//ordMtx    []*kvs.Content

	//locks map[string]*keyLock
	locks sync.Map
	//txs   map[kvs.TxID]*txState
	txs sync.Map

	debug      bool
	debugError bool
}

func NewKVService() *KVService {
	kv := &KVService{}
	kv.mp = sync.Map{}
	kv.txs = sync.Map{}
	kv.locks = sync.Map{}
	kv.lastPrint = time.Now()
	kv.stats.Init()
	kv.prevStats.Init()
	kv.debug = false
	kv.debugError = false
	return kv
}

func (kv *KVService) DebugPrintKeys() {
	kv.txs.Range(func(key any, value any) bool {
		fmt.Printf("%s\n", key)
		return true
	})
}

func (kv *KVService) DoBadTxID(txid kvs.TxID) {
	kv.stats.abort_error.Add(1)
	if kv.debugError {
		kv.mu.Lock()
		fmt.Printf("\nBAD TXID: %s\nCURRENT KEYS:\n", txid)
		kv.DebugPrintKeys()
		kv.mu.Unlock()
		return
	}
}

func newKeyLock(key string) *keyLock {
	return &keyLock{
		key:         key,
		readers:     sync.Map{},
		readerCount: &atomic.Uint32{},
	}
}

func (kv *KVService) getTx(txid kvs.TxID) (*txState, bool) {
	tmp, err := kv.txs.Load(txid)
	if !err {
		return nil, err
	}
	return tmp.(*txState), err
}

func (tx *txState) getSLockFromKey(key string) (*keyLock, bool) {
	tmp, err := tx.s_held.Load(key)
	if !err {
		return nil, err
	}
	return tmp.(*keyLock), err
}

func (tx *txState) getXLockFromKey(key string) (*keyLock, bool) {
	tmp, err := tx.x_held.Load(key)
	if !err {
		return nil, err
	}
	return tmp.(*keyLock), err
}

func (kv *KVService) Batch(request *kvs.RequestBatch, response *kvs.ResponseBatch) error {
	response.Values = make([]string, len(request.Ops))
	//kv.Lock()
	//defer kv.Unlock()
	var localGets, localPuts uint64

	for i, op := range request.Ops {
		if op.IsRead() {
			if v, ok := kv.mp.Load(op.Key); ok {
				switch valueVal := v.(type) {
				case string:
					response.Values[i] = valueVal
				case *kvs.Content:
					response.Values[i] = valueVal.Value
				}
				//response.Values[i] = v.(string)
			}
			//kv.stats.gets.Add(1)
			localGets++

		} else if op.IsWrite() {
			kv.mp.Store(op.Key, op.Value)
			//kv.stats.puts.Add(1)
			localPuts++
		}
	}
	if localGets > 0 {
		kv.stats.gets.Add(localGets)
	}
	if localPuts > 0 {
		kv.stats.puts.Add(localPuts)
	}
	return nil
}

func (kv *KVService) getOrCreateContent(key string) *kvs.Content {
	c, _ := kv.mp.LoadOrStore(key, &kvs.Content{Key: key})
	return c.(*kvs.Content)
}

func (kv *KVService) getOrCreateLockState(key string) *keyLock {
	lok, _ := kv.locks.LoadOrStore(key, newKeyLock(key))
	return lok.(*keyLock)
}

// returns nil if state with matching id already exists
func (kv *KVService) CreateTxState(txid kvs.TxID) *txState {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	state, ok := kv.getTx(txid)
	if ok {
		return nil
	}
	state = &txState{
		abort:              false,
		writes:             sync.Map{},
		s_held:             sync.Map{},
		x_held:             sync.Map{},
		readWriteLockStrat: 1,
		readState:          sync.Map{},
	}
	//copy all values into readState
	kv.mp.Range(func(key, value any) bool {
		state.readState.Store(key, value)
		return true
	})
	kv.txs.Store(txid, state)
	return state
}

// non blocking lock requests, grant the lock if its safe to do so
func (kv *KVService) tryAcquireS(tx *txState, key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lok := kv.getOrCreateLockState(key)
	if lok.writer != nil && lok.writer != tx {
		kv.stats.abort_retry.Add(1)
		return false
	}
	lok.readers.Store(tx, nil)
	lok.readerCount.Add(1)
	tx.s_held.Store(key, lok)
	return true
}

// try to acquire a write lock if there are no other readers
func (kv *KVService) tryAcquireX(tx *txState, key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lok := kv.getOrCreateLockState(key)
	if lok.writer == tx {
		//already acquired
		return true
	}
	if lok.writer != nil && lok.writer != tx {
		//different writer
		kv.stats.abort_retry.Add(1)
		return false
	}
	if lok.readerCount.Load() > 0 {
		//we must check that it holds a read lock
		if _, ok := lok.readers.Load(tx); !ok || (tx.readWriteLockStrat == 1 && lok.readerCount.Load() > 1) {
			//if not only reader, abort self
			kv.stats.abort_retry.Add(1)
			return false
		} else if lok.readerCount.Load() > 1 && tx.readWriteLockStrat == 2 {
			lok.readers.Range(func(k, value any) bool {
				//if not only reader, abort all other readers
				lok.readers.Delete(k)
				lok.readerCount.Store(lok.readerCount.Load() - 1)
				k.(*txState).s_held.Delete(key)
				k.(*txState).abort = true
				return true
			})
		} else {
			if tx.readWriteLockStrat == 3 {
				//if using snapshots, only allow write if snapshot == current
				tmp, ok := tx.readState.Load(key)
				live, ok2 := kv.mp.Load(key)
				if ok && ok2 && tmp != live {
					kv.stats.abort_retry.Add(1)
					return false
				}
			}
			lok.readers.Delete(tx)
			lok.readerCount.Store(lok.readerCount.Load() - 1)
			tx.s_held.Delete(key)
		}
	}
	lok.writer = tx
	tx.x_held.Store(key, lok)
	return true
}

// transaction clean up- drop every lock that tx holds and remove its staged state. why?
// so other transactions can continue and locks or memory is leaked
func (kv *KVService) txCleanUp(txid kvs.TxID) {
	state, ok := kv.getTx(txid)
	if ok {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		state.x_held.Range(func(k any, v any) bool {
			//remove self from all write locks
			vl := v.(*keyLock)
			if vl.writer == state {
				vl.writer = nil
			}
			return true
		})
		state.s_held.Range(func(k any, v any) bool {
			//remove self from all read locks
			vl := v.(*keyLock)
			vl.readers.Delete(state)
			vl.readerCount.Store(vl.readerCount.Load() - 1)
			return true
		})
		kv.txs.Delete(txid)
		_, nok := kv.txs.Load(txid)
		if nok {
			panic("Key was not deleted!")
		}
	}
}
func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.stats.gets.Add(1)
	if value, ok := kv.mp.Load(request.Key); ok {
		switch valueVal := value.(type) {
		case string:
			response.Value = valueVal
		case *kvs.Content:
			response.Value = valueVal.Value
		}
		//response.Value = value.(string)
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.Response) error {
	kv.stats.puts.Add(1)
	kv.mp.Store(request.Key, request.Value)
	response.Error = ""
	return nil

	//kv.PutAndCheck(request.Key, request.Value)
}

// TX RPCs
// read tx and check if it holds S or X.
// return a staged value if there is one
func (kv *KVService) TxGet(request *kvs.TxGetRequest, response *kvs.GetResponse) error {
	state, sOk := kv.getTx(request.GetTxID())
	if !sOk {
		response.Error = kvs.ERROR_BAD_TXID
		kv.DoBadTxID(request.GetTxID())
		return nil
	}
	if state.abort {
		response.Error = kvs.ERROR_SERVER_ABT
		return nil
	}
	if kv.debug {
		kv.mu.Lock()
		fmt.Printf("GET id: \n%s\n", request.GetTxID())
		kv.mu.Unlock()
	}
	//if we have a previous read, read from self
	if v, ok := state.writes.Load(request.Key); ok {
		kv.stats.gets.Add(1)
		response.Value, response.Error = v.(string), ""
		return nil
	}

	//attempt to get read lock
	if !kv.tryAcquireS(state, request.Key) {
		if _, x := state.getXLockFromKey(request.Key); !x {
			response.Error = kvs.ERROR_S_LOCK_FAIL
			return nil
		}
	}

	response.Error = ""
	if state.readWriteLockStrat == 3 {
		c, ok := state.readState.Load(request.Key)
		if ok {
			response.Value = c.(*kvs.Content).Value
		} else {
			response.Value = ""
		}
	} else {
		c := kv.getOrCreateContent(request.Key)
		response.Value = c.Value
	}
	kv.stats.gets.Add(1)
	return nil
}

func (kv *KVService) TxPut(request *kvs.TxPutRequest, response *kvs.Response) error {
	state, sOk := kv.getTx(request.GetTxID())
	if !sOk {
		kv.DoBadTxID(request.GetTxID())
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	if state.abort {
		response.Error = kvs.ERROR_SERVER_ABT
		return nil
	}
	if kv.debug {
		kv.mu.Lock()
		fmt.Printf("PUT id: \n%s\n", request.GetTxID())
		kv.mu.Unlock()
	}
	if !kv.tryAcquireX(state, request.Key) {
		response.Error = kvs.ERROR_X_LOCK_FAIL
		return nil
	}
	kv.stats.puts.Add(1)
	state.writes.Store(request.Key, request.Value)
	response.Error = ""
	return nil
}

func (kv *KVService) TxCommit(request *kvs.TxRequest, response *kvs.Response) error {
	//mutex locking unnecessary because if we are committing then we have proper lock access
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	state, ok := kv.getTx(request.GetTxID())
	if !ok {
		kv.DoBadTxID(request.GetTxID())
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	if state.abort {
		response.Error = kvs.ERROR_SERVER_ABT
		return nil
	}
	if kv.debug {
		kv.mu.Lock()
		fmt.Printf("COMMIT id: \n%s\n", request.GetTxID())
		kv.mu.Unlock()
	}

	state.writes.Range(func(k any, v any) bool {
		c := kv.getOrCreateContent(k.(string))
		c.SetContent(v.(string))
		return true
	})

	kv.txCleanUp(request.GetTxID())
	kv.stats.commits.Add(1)
	response.Error = ""
	return nil
}

// drop the staged write and release the locks
func (kv *KVService) TxAbort(request *kvs.TxRequest, response *kvs.Response) error {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if kv.debug {
		kv.mu.Lock()
		fmt.Printf("ABORT id: \n%s\n", request.GetTxID())
		kv.mu.Unlock()
	}
	kv.txCleanUp(request.GetTxID())
	kv.stats.aborts.Add(1)
	response.Error = ""
	return nil
}

// initiate a transaction
func (kv *KVService) TxBegin(request *kvs.TxRequest, response *kvs.Response) error {
	if kv.debug {
		kv.mu.Lock()
		fmt.Printf("BEGIN id: \n%s\n", request.GetTxID())
		kv.mu.Unlock()
	}
	if !request.GetTxID().IsValid() {
		kv.DoBadTxID(request.GetTxID())
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	if kv.CreateTxState(request.GetTxID()) == nil {
		kv.DoBadTxID(request.GetTxID())
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	response.Error = ""
	return nil
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}

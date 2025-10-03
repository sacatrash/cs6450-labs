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

type Stats struct {
	puts    *atomic.Uint64
	gets    *atomic.Uint64
	commits *atomic.Uint64
	aborts  *atomic.Uint64
}

type keyLock struct {
	key string
	//readers map[*txState]any
	readers     sync.Map
	readerCount *atomic.Uint32
	writer      *txState
}

type txState struct {
	id kvs.TxID
	//writes map[string]string
	writes sync.Map
	//s_held map[string]*keyLock
	s_held sync.Map
	//x_held map[string]*keyLock
	x_held sync.Map
}

// use the KVService mutex when we need to add/remove keys
// use the mutexMap mutex for getting/setting existing keys
type KVService struct {
	//sync.Mutex
	//mp        map[string]*atomic.Value
	mu        sync.Mutex //mutex used during lock acquisition/release
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
	//ordMtx    []*kvs.Content

	//locks map[string]*keyLock
	locks sync.Map
	//txs   map[kvs.TxID]*txState
	txs sync.Map
}

func (kv *KVService) getTx(txid kvs.TxID) (*txState, bool) {
	tmp, err := kv.txs.Load(txid)
	return tmp.(*txState), err
}

func (tx *txState) getSLockFromKey(key string) (*keyLock, bool) {
	tmp, err := tx.s_held.Load(key)
	return tmp.(*keyLock), err
}

func (tx *txState) getXLockFromKey(key string) (*keyLock, bool) {
	tmp, err := tx.x_held.Load(key)
	return tmp.(*keyLock), err
}

func (s *Stats) Init() {
	s.puts = new(atomic.Uint64)
	s.gets = new(atomic.Uint64)
	s.commits = new(atomic.Uint64)
	s.aborts = new(atomic.Uint64)
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

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = new(atomic.Uint64)
	r.gets = new(atomic.Uint64)
	r.commits = new(atomic.Uint64)
	r.aborts = new(atomic.Uint64)
	r.puts.Store(s.puts.Load() - prev.puts.Load())
	r.gets.Store(s.gets.Load() - prev.gets.Load())
	r.commits.Store(s.commits.Load() - prev.commits.Load())
	r.aborts.Store(s.aborts.Load() - prev.aborts.Load())
	return r
}

// EDITED
func NewKVService() *KVService {
	kv := &KVService{}
	kv.mp = sync.Map{}
	kv.txs = sync.Map{}
	kv.locks = sync.Map{}
	kv.lastPrint = time.Now()
	kv.stats.Init()
	kv.prevStats.Init()
	return kv
}

func (kv *KVService) getOrCreateContent(key string) *kvs.Content {
	c, _ := kv.mp.LoadOrStore(key, &kvs.Content{Key: key})
	return c.(*kvs.Content)
}

func (kv *KVService) getOrCreateLockState(key string) *keyLock {
	lok, ok := kv.locks.Load(key)
	if !ok {
		lok = &keyLock{key: key, readers: sync.Map{}}
		kv.locks.Store(key, lok)
	}
	return lok.(*keyLock)
}

func (kv *KVService) CreateTxState(txid kvs.TxID) *txState {
	state, ok := kv.getTx(txid)
	if ok {
		kv.txCleanUp(state.id)
	}
	state = &txState{
		id:     txid,
		writes: sync.Map{},
		s_held: sync.Map{},
		x_held: sync.Map{},
	}
	kv.txs.Store(txid, state)
	return state
}

// non blocking lock requests, grant the lock if its safe to do so
func (kv *KVService) tryAcquireS(tx *txState, key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lok := kv.getOrCreateLockState(key)
	if lok.writer != nil && lok.writer != tx {
		return false
	}
	lok.readers.Store(tx, nil)
	tx.s_held.Store(key, lok)
	return true
}

/*
 */
func (kv *KVService) tryAcquireX(tx *txState, key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lok := kv.getOrCreateLockState(key)
	if lok.writer == tx {
		return true
	}
	if lok.writer != nil && lok.writer != tx {
		return false
	}
	if lok.readerCount.Load() > 0 {
		if _, ok := lok.readers.Load(tx); !ok {
			return false
		}
		lok.readers.Delete(tx)
		lok.readerCount.Store(lok.readerCount.Load() - 1)
		tx.s_held.Delete(key)
	}
	lok.writer = tx
	tx.x_held.Store(key, tx)
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
			return true
		})
		kv.txs.Delete(txid)
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

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.stats.puts.Add(1)
	kv.mp.Store(request.Key, request.Value)
	response.Error = nil
	return nil

	//kv.PutAndCheck(request.Key, request.Value)
}

// TX RPCs
// read tx and check if it holds S or X.
// return a staged value if there is one
func (kv *KVService) TxGet(request *kvs.TxGetRequest, response *kvs.TxGetResponse) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	state, sOk := kv.getTx(request.Tx)
	if !sOk {
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	//if we have write lock, clear to proceed wit hread
	if v, ok := state.writes.Load(request.Key); ok {
		kv.stats.gets.Add(1)
		response.Value, response.Error = v.(string), nil
		return nil
	}

	//attempt to get read lock
	if !kv.tryAcquireS(state, request.Key) {
		if _, x := state.getXLockFromKey(request.Key); !x {
			response.Error = kvs.ERROR_S_LOCK_FAIL
			return nil
		}
	}

	response.Error = nil
	c := kv.getOrCreateContent(request.Key)
	kv.stats.gets.Add(1)
	response.Value = c.Value
	return nil
}

func (kv *KVService) TxPut(request *kvs.TxPutRequest, response *kvs.TxPutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	state, sOk := kv.getTx(request.Tx)
	if !sOk {
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}
	if !kv.tryAcquireX(state, request.Key) {
		response.Error = kvs.ERROR_X_LOCK_FAIL
		return nil
	}
	kv.stats.puts.Add(1)
	state.writes.Store(request.Key, request.Value)
	response.Error = nil
	return nil
}

func (kv *KVService) TxCommit(request *kvs.TxCommitRequest, response *kvs.TxCommitResponse) error {
	//mutex locking unnecessary because if we are committing then we have proper lock access
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	state, ok := kv.getTx(request.Tx)
	if !ok {
		response.Error = kvs.ERROR_BAD_TXID
		return nil
	}

	state.writes.Range(func(k any, v any) bool {
		c := kv.getOrCreateContent(k.(string))
		c.SetContent(v.(string))
		return true
	})

	kv.txCleanUp(request.Tx)
	kv.stats.commits.Add(1)
	response.Error = nil
	return nil
}

// drop the staged write and release the locks
func (kv *KVService) TxAbort(request *kvs.TxAbortRequest, response *kvs.TxAbortResponse) error {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	kv.txCleanUp(request.Tx)
	kv.stats.aborts.Add(1)
	response.Error = nil
	return nil
}

// initiate a transaction
func (kv *KVService) TxBegin(request *kvs.TxPrepareRequest, response *kvs.TxPrepareResponse) error {
	kv.CreateTxState(request.Tx)
	response.Error = nil
	return nil
}

func (kv *KVService) printStats() {
	//kv.Lock()
	//locks no longer needed as we're using atomics for it now
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = Stats{}
	kv.prevStats.Init()
	kv.prevStats.gets.Store(stats.gets.Load())
	kv.prevStats.puts.Store(stats.puts.Load())
	kv.prevStats.commits.Store(stats.commits.Load())
	kv.prevStats.aborts.Store(stats.aborts.Load())
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	//kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	gets := diff.gets.Load()
	puts := diff.puts.Load()
	commits := diff.commits.Load()
	aborts := diff.aborts.Load()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\ncommits/s %0.2f\naborts/s %0.2f\n\n",
		float64(gets)/deltaS,
		float64(puts)/deltaS,
		float64(gets+puts)/deltaS,
		float64(commits)/deltaS,
		float64(aborts)/deltaS)
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

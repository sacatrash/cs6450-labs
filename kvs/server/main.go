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
	readers map[string]struct{}
	writer  string
}

type txState struct {
	writes map[string]string
	s_held map[string]struct{}
	x_held map[string]struct{}
	active bool
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
		if op.IsRead {
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

		} else {
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

// use the KVService mutex when we need to add/remove keys
// use the mutexMap mutex for getting/setting existing keys
type KVService struct {
	//sync.Mutex
	//mp        map[string]*atomic.Value
	mu        sync.Mutex
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
	ordMtx    []*kvs.Content

	locks map[string]*keyLock
	txs   map[string]*txState
}

// EDITED
func NewKVService() *KVService {
	kvs := &KVService{}
	//kvs.mp = make(map[string]*atomic.Value)
	//kvs.mp = sync.Map{}
	kvs.txs = map[string]*txState{}
	kvs.locks = map[string]*keyLock{}
	kvs.lastPrint = time.Now()
	kvs.stats.Init()
	kvs.prevStats.Init()
	return kvs
}

// CHECK THIS, EDIT IT MORE?
// will return the commited value holder for key. if the key does not
// exist , one will be created and will store it in the sync map
func (kv *KVService) getOrCreateCommitt(key string) *kvs.Content {
	if v, ok := kv.mp.Load(key); ok {
		switch valueVal := v.(type) {
		case *kvs.Content:
			return valueVal
		case string:
			c := &kvs.Content{Order: len(kv.ordMtx), Value: valueVal}
			kv.mp.Store(key, c)
			kv.ordMtx = append(kv.ordMtx, c)
			return c
		}

		/*
		   if s, ok := v.(string); ok {
		       c := &kvs.Content{Order: len(kv.ordMtx), Value: s}
		       kv.mp.Store(key, c)
		       kv.ordMtx = append(kv.ordMtx, c)
		       return c
		   }*/
	}
	c := &kvs.Content{Order: len(kv.ordMtx)}
	kv.mp.Store(key, c)
	kv.ordMtx = append(kv.ordMtx, c)
	return c
}

func (kv *KVService) getOrCreateLockState(key string) *keyLock {
	lok, ok := kv.locks[key]
	if !ok {
		lok = &keyLock{readers: map[string]struct{}{}}
		kv.locks[key] = lok
	}
	return lok
}

func (kv *KVService) getOrCreateTxState(txid string) *txState {
	state, ok := kv.txs[txid]
	if !ok {
		state = &txState{
			writes: map[string]string{},
			s_held: map[string]struct{}{},
			x_held: map[string]struct{}{},
			active: true,
		}
		kv.txs[txid] = state
	}
	return state
}

// non blocking lock requests, grant the lock if its safe to do so
func (kv *KVService) try_S(txid, key string) bool {
	lok := kv.getOrCreateLockState(key)
	if lok.writer != "" && lok.writer != txid {
		return false
	}
	lok.readers[txid] = struct{}{}
	kv.getOrCreateTxState(txid).s_held[key] = struct{}{}
	return true
}

/*
 */
func (kv *KVService) try_X(txid, key string) bool {
	lok := kv.getOrCreateLockState(key)
	if lok.writer == txid {
		return true
	}
	if lok.writer != "" && lok.writer != txid {
		return false
	}
	if len(lok.readers) > 0 {
		if _, ok := lok.readers[txid]; !ok {
			return false
		}
		delete(lok.readers, txid)
		delete(kv.getOrCreateTxState(txid).s_held, key)
	}
	lok.writer = txid
	kv.getOrCreateTxState(txid).x_held[key] = struct{}{}
	return true
}

// transaction clean up- drop every lock that tx holds and remove its staged state. why?
// so other transactions can continue and locks or memory is leaked
func (kv *KVService) txCleanUp(txid string) {
	state, ok := kv.txs[txid]
	if ok {
		for k := range state.s_held {
			delete(kv.getOrCreateLockState(k).readers, txid)
		}
		for k := range state.x_held {
			if kv.getOrCreateLockState(k).writer == txid {
				kv.getOrCreateLockState(k).writer = ""
			}
		}
		delete(kv.txs, txid)
	}
}
func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.stats.gets.Add(1)
	/*vlk, ok := kv.mutexMap[request.Key]

	if ok {
		vlk.Lock()
		defer vlk.Unlock()
	} else {
		return nil
	}*/

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

func (kv *KVService) GetNewOrder() int {
	return len(kv.ordMtx)
}

/* //helper function probably not needed
func (kv *KVService) PutAndCheck(key string, value string) {
	v, ok = kv.mp.Get(key)
	if(!ok) {
		tmp := Content{Order: kv.GetNewOrder(), Value: value}
		kv.mp.Store(key, tmp)
		kv.ordMtx = append(kv.ordMtx, tmp)
	}
	else {
		v.setContent(value)
	}
}
*/

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.stats.puts.Add(1)
	kv.mp.Store(request.Key, request.Value)
	response.Ok = true
	return nil

	//kv.PutAndCheck(request.Key, request.Value)
}

// TX RPCs
// read tx and check if it holds S or X.
// return a staged value if there is one
func (kv *KVService) TxGet(request *kvs.TxGetRequest, response *kvs.TxGetResponse) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.stats.gets.Add(1)
	state := kv.getOrCreateTxState(string(request.Tx))
	if v, ok := state.writes[request.Key]; ok {
		response.Value, response.Ok = v, true
		return nil
	}

	if _, r := state.s_held[request.Key]; !r {
		if _, x := state.x_held[request.Key]; !x {
			response.Ok = false
			return nil
		}
	}

	response.Ok = true
	c := kv.getOrCreateCommitt(request.Key)
	response.Value = c.Value
	return nil
}

func (kv *KVService) TxPut(request *kvs.TxPutRequest, response *kvs.TxPutResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.stats.puts.Add(1)
	state := kv.getOrCreateTxState(string(request.Tx))
	if _, ok := state.x_held[request.Key]; !ok {
		response.Ok = false
		return nil
	}
	state.writes[request.Key] = request.Value
	response.Ok = true
	return nil
}

func (kv *KVService) TxCommit(request *kvs.TxCommitRequest, response *kvs.TxCommitResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	state, ok := kv.txs[string(request.Tx)]
	if !ok {
		response.Ok = true
		return nil
	}

	for k, v := range state.writes {
		c := kv.getOrCreateCommitt(k)
		c.Lock()
		c.SetContent(v)
		c.Unlock()
	}
	kv.txCleanUp(string(request.Tx))
	kv.stats.commits.Add(1)
	response.Ok = true
	return nil
}

// drop the staged write and release the locks
func (kv *KVService) TxAbort(request *kvs.TxAbortRequest, response *kvs.TxAbortResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.txCleanUp(string(request.Tx))
	kv.stats.aborts.Add(1)
	response.Ok = true
	return nil
}

// get all of the requested locks per shard
func (kv *KVService) TxPrepare(request *kvs.TxPrepareRequest, response *kvs.TxPrepareResponse) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, item := range request.Items {
		if item.Mode == kvs.Lock_X {
			if !kv.try_X(string(request.Tx), item.Key) {
				kv.txCleanUp(string(request.Tx))
				response.Ok = false
				return nil
			}

		} else {
			if !kv.try_S(string(request.Tx), item.Key) {
				kv.txCleanUp(string(request.Tx))
				response.Ok = false
				return nil
			}
		}
	}
	response.Ok = true
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

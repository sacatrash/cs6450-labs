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
	puts atomic.Uint64
	gets atomic.Uint64
}

func (kv *KVService) Batch(request *kvs.RequestBatch, response *kvs.ResponseBatch) error {
	response.Values = make([]string, len(request.Ops))

	name := request.Name
	if _, ok := kv.q.Node2queue[name]; !ok {
		kv.Lock()
		kv.q.AddNewQueue(name)
		kv.Unlock()
	}

	res := <-*kv.q.Node2queue[name].AddTask(request, response)

	for _, op := range request.Ops {
		if op.IsRead {
			kv.stats.gets.Add(1)

		} else {
			kv.stats.puts.Add(1)
		}
	}
	return res
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts.Store(s.puts.Load() - prev.puts.Load())
	r.gets.Store(s.gets.Load() - prev.gets.Load())
	return r
}

// use the KVService mutex when we need to add/remove keys
// use the mutexMap mutex for getting/setting existing keys
type KVService struct {
	sync.Mutex
	q         kvs.MasterQueue
	mp        map[string]string
	mutexMap  map[string]sync.Mutex
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]string)
	kvs.mutexMap = make(map[string]sync.Mutex)
	kvs.lastPrint = time.Now()
	kvs.q.Initialize(&kvs.Mutex, &kvs.mp)
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	//vlk, ok := kv.mutexMap[request.Key]
	kv.stats.gets.Add(1)
	kv.Lock()
	defer kv.Unlock()
	/*if ok {
		vlk.Lock()
		defer vlk.Unlock()
	} else {
		return nil
	}*/

	if value, found := kv.mp[request.Key]; found {
		response.Value = value
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	//vlk, ok := kv.mutexMap[request.Key]
	kv.stats.puts.Add(1)
	/*if ok {
		vlk.Lock()
		defer vlk.Unlock()
	} else {
		kv.Lock()
		defer kv.Unlock()

		kv.mutexMap[request.Key] = sync.Mutex{}
	}*/
	kv.Lock()
	defer kv.Unlock()

	kv.mp[request.Key] = request.Value

	return nil
}

func (kv *KVService) printStats() {
	//kv.Lock()
	//locks no longer needed as we're using atomics for it now
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	//kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	gets := diff.gets.Load()
	puts := diff.puts.Load()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(gets)/deltaS,
		float64(puts)/deltaS,
		float64(gets+puts)/deltaS)
}

func (kv *KVService) processQueue() {
	kv.q.Process()
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
			kvs.processQueue()
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}

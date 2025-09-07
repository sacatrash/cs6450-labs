package main

import (
	//"hash/fnv"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	pb "github.com/rstutsman/cs6450-labs/kvs/proto"

	"google.golang.org/grpc"
)

type Stats struct {
	puts *atomic.Uint64
	gets *atomic.Uint64
}

// use the KVService mutex when we need to add/remove keys
// use the mutexMap mutex for getting/setting existing keys
type KVService struct {
	pb.UnimplementedKVServiceServer
	//mp        map[string]*atomic.Value
	mp        sync.Map
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func (s *Stats) Init() {
	s.puts = new(atomic.Uint64)
	s.gets = new(atomic.Uint64)
}

// func (kv *KVService) Batch(request *kvs.RequestBatch, response *kvs.ResponseBatch) error {
func (kv *KVService) Batch(_ context.Context, in *pb.BatchRequest) (*pb.BatchReply, error) {
	list := in.GetOpList()
	reply := pb.BatchReply{Values: make([]string, len(list))}
	//kv.Lock()
	//defer kv.Unlock()

	for i, op := range list {
		if op.GetIsRead() {
			if v, ok := kv.mp.Load(op.Key); ok {
				reply.Values[i] = v.(string)
			}
			kv.stats.gets.Add(1)

		} else {
			kv.mp.Store(op.Key, op.Value)
			kv.stats.puts.Add(1)
		}
	}
	return &reply, nil
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = new(atomic.Uint64)
	r.gets = new(atomic.Uint64)
	r.puts.Store(s.puts.Load() - prev.puts.Load())
	r.gets.Store(s.gets.Load() - prev.gets.Load())
	return r
}

func NewKVService() *KVService {
	kvs := &KVService{}
	//kvs.mp = make(map[string]*atomic.Value)
	kvs.mp = sync.Map{}
	kvs.lastPrint = time.Now()
	kvs.stats.Init()
	kvs.prevStats.Init()
	return kvs
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

	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.stats.puts.Add(1)

	kv.mp.Store(request.Key, request.Value)

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

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvs := NewKVService()
	//rpc.Register(kvs)
	//rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)
	s := grpc.NewServer()
	pb.RegisterKVServiceServer(s, kvs)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	if err := s.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	//http.Serve(l, nil)
}

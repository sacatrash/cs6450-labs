
package main

import (
	"sync"
	"hash/fnv"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

    "github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)


type perHost struct {
    c        *Client
    active   []kvs.Op
    spare    []kvs.Op
    deadline time.Time
    sendq    chan []kvs.Op
}

type Client struct {
	rpcClient *rpc.Client
    Name string
    Dest string
}

func hashKey(s string) uint32 {
    h := fnv.New32a()
    _, _ = h.Write([]byte(s))
    return h.Sum32()
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient, uuid.New().String(), addr}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func (client *Client) Batch(ops []kvs.Op) []string {
	request, response := kvs.RequestBatch{Ops: ops, Src: client.Name, Dest: client.Dest}, kvs.ResponseBatch{}
	if err := client.rpcClient.Call("KVService.Batch", &request, &response); err != nil {
		log.Fatal(err)
	}
	return response.Values
}


func TxGetRPC(rc *rpc.Client, txid string, key string) (string, bool){
    //request := txGetRequest{Tx: TxID(txid), Key:key}
    request := kvs.txGetRequest{Tx: kvs.TxID(txid), Key:key}
    //var response txGetResponse
    var response kvs.txGetResponse
    //for a RPC error , lets have the caller abort and retry
    if err := rc.Call("KVService.TxGet", &request, &response); err != nil{
        return "", false
    }

    if !response.Ok{
        return "", false
    }
    return response.Value, true
}


func TxPutRPC(rc *rpc.Client, txid string, key, value string) bool{
    //request := txPutRequest{Tx: TxID(txid), Key:key, Value: value}
    request := kvs.txPutRequest{Tx: kvs.TxID(txid), Key:key, Value: value}
    //var response txPutResponse
    var response kvs.txPutResponse

    if err := rc.Call("KVService.TxPut", &request, &response); err != nil{
        return false
    }
    return response.Ok
}


//2PC train of thought, when this is called, the client will ask each. server to commit
//if it fails, client can send txabortrpc - AKA 2 phase commit
func TxCommitRPC(rc *rpc.Client, txid string, lead bool) bool{
    //request := txCommitRequest{Tx: TxID(txid), Lead: lead}
    request := kvs.txCommitRequest{Tx: kvs.TxID(txid), Lead: lead}
    //var response txCommitResponse
    var response kvs.txCommitResponse
    if err := rc.Call("KVService.TxCommit", &request, &response); err !=nil{
        return false
    }
    return response.Ok
}



func TxAbortRPC(rc *rpc.Client, txid string) bool{
    //request := txAbortRequest{Tx: TxID(txid)}
    request := kvs.txAbortRequest{Tx: kvs.TxID(txid)}
    //var response txAbortResponse
    var response kvs.txAbortResponse
    if err := rc.Call("KVService.TxAbort", &request, &response); err != nil{
        return false
    }
    return true
}



/* ##############################*/

func runClient(id int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
    batchSize := 4096
    ttlFlush := time.Millisecond

    hosts := make([]*perHost, len(addrs))
    var wg sync.WaitGroup
    var opsDone atomic.Uint64

    
    for i, addr := range addrs {
        h := &perHost{
            c:        Dial(addr),
            active:   make([]kvs.Op, 0, batchSize),
            spare:    make([]kvs.Op, 0, batchSize),
            deadline: time.Now().Add(ttlFlush),
            sendq:    make(chan []kvs.Op, 2), 
        }
        hosts[i] = h

        wg.Add(1)
        go func(h *perHost) {
            defer wg.Done()
            for b := range h.sendq {
                res := h.c.Batch(b)             
                opsDone.Add(uint64(len(res)))   
            }
        }(h)
    }

    value := strings.Repeat("x", 128)

    flushOne := func(h *perHost) {
        if len(h.active) == 0 { return }
        full := h.active
        h.active, h.spare = h.spare[:0], full
        h.deadline = time.Now().Add(ttlFlush)
        h.sendq <- full
    }

    flushExpired := func() {
        now := time.Now()
        for _, h := range hosts {
            if now.After(h.deadline) && len(h.active) > 0 {
                flushOne(h)
            }
        }
    }

    for !done.Load() {
        for j := 0; j < 4096 && !done.Load(); j++ {
            op := workload.Next()
            key := strconv.FormatUint(op.Key, 10)
            shard := int(hashKey(key) % uint32(len(hosts)))
            h := hosts[shard]

            if op.IsRead {
                h.active = append(h.active, kvs.Op{IsRead: true, Key: key})
            } else {
                h.active = append(h.active, kvs.Op{IsRead: false, Key: key, Value: value})
            }
            if len(h.active) >= cap(h.active) {
                flushOne(h)
            }
        }
        flushExpired()
    }

    for _, h := range hosts { flushOne(h) }
    for _, h := range hosts { close(h.sendq) }
    wg.Wait()

    fmt.Printf("Client %d finished operations.\n", id)
    resultsCh <- opsDone.Load()
}



type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}
/*EDIT main*/
func main() {
    hosts := HostList{}

    flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
    theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
    //INCLUDE XFER BELOW
    workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
    //addition
    host_generators := flag.Int("host_generators", 2, "generators per host")

    secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
    flag.Parse()

    if len(hosts) == 0 {
        hosts = append(hosts, "localhost:8080")
    }

    fmt.Printf(
        "hosts %v\n"+
            "theta %.2f\n"+
            "workload %s\n"+
            "secs %d\n",
        hosts, *theta, *workload, *secs,
    )


    start := time.Now()

    done := atomic.Bool{}
    //resultsCh := make(chan uint64)
    resultsCh := make(chan uint64, len(hosts)*(*host_generators))
    /*
        host := hosts[0]
        clientId := 0
        go func(clientId int) {
            workload := kvs.NewWorkload(*workload, *theta)
            runClient(clientId, host, &done, workload, resultsCh)
        }(clientId)
    */
    /*
        for i, host := range hosts {
            clientId := i
            go func(host string , clientId int) {
                workload := kvs.NewWorkload(*workload , *theta)
                runClient(clientId, host, &done, workload, resultsCh)
            }(host, clientId)
        }
    */

    for i := range hosts {
        for g := 0; g < *host_generators; g++ {
            clientId := i*(*host_generators) + g

            go func(clientId int, addrs []string) {
                work_load := kvs.NewWorkload(*workload, *theta)
                runClient(clientId, addrs, &done, work_load, resultsCh)
            }(clientId, hosts)
        }
    }

    time.Sleep(time.Duration(*secs) * time.Second)
    done.Store(true)

    //opsCompleted := <-resultsCh
    /*
        var opsCompleted uint64
        for range hosts {
            opsCompleted += <- resultsCh
        }
    */
    var opsCompleted uint64
    for i := 0; i < len(hosts)*(*host_generators); i++ {
        opsCompleted += <-resultsCh
    }

    elapsed := time.Since(start)

    opsPerSec := float64(opsCompleted) / elapsed.Seconds()
    fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
package main

import (
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

type Client struct {
	rpcClient *rpc.Client
	Name      string
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient, uuid.New().String()}
}

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{}
	request.Request.Key = key
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
	request, response := kvs.RequestBatch{Ops: ops, Name: client.Name}, kvs.ResponseBatch{}
	if err := client.rpcClient.Call("KVService.Batch", &request, &response); err != nil {
		log.Fatal(err)
	}
	return response.Values
}

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024
	const ttlFlush = time.Millisecond
	batch := make([]kvs.Op, 0, 64)
	deadline := time.Now().Add(ttlFlush)

	opsCompleted := uint64(0)

	flushBatch := func() {
		results := client.Batch(batch)
		opsCompleted += uint64(len(results))
		batch = batch[:0]
		deadline = time.Now().Add(ttlFlush)
	}

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			//key := fmt.Sprintf("%d", op.Key)
			key := strconv.FormatUint(op.Key, 10)
			/*
				if op.IsRead {
					client.Get(key)
				} else {
					client.Put(key, value)
				}
				opsCompleted++
			*/
			if op.IsRead {
				batch = append(batch, kvs.Op{IsRead: true, Key: key})

			} else {
				batch = append(batch, kvs.Op{IsRead: false, Key: key, Value: value})

			}

			if len(batch) >= cap(batch) || time.Now().After(deadline) {
				flushBatch()
			}
			if done.Load() {
				break
			}
		}
	}
	if len(batch) > 0 {
		flushBatch()
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
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

	for i, host := range hosts {
		for g := 0; g < *host_generators; g++ {
			clientId := i*(*host_generators) + g
			go func(host string, clientId int) {
				work_load := kvs.NewWorkload(*workload, *theta)
				runClient(clientId, host, &done, work_load, resultsCh)
			}(host, clientId)
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

package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func (cli *Client) getShard(key string) *kvs.ServerClientConn {
	return cli.Hosts[kvs.ShardForKey(key, len(cli.Hosts))]
}

func Dial(addr string) *kvs.ServerClientConn {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &kvs.ServerClientConn{RpcClient: rpcClient, Dest: addr}
}

/*EDIT main*/
func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	//INCLUDE XFER BELOW
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C, Accounting)")
	//addition
	defaultHosts := 2
	host_generators := flag.Int("host_generators", defaultHosts, "generators per host")

	secs := flag.Int("secs", 30, "Duration in seconds for each client to run. Set to 0 for infinite run.")
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
				var work_load kvs.DefaultWorkload
				if *workload == "Accounting" {
					work_load = kvs.NewAccountingWorkload(uint64(clientId), uint64(len(hosts)**host_generators), 100, 50)
				} else {
					work_load = kvs.NewTxnWorkload(*workload, *theta)
				}
				RunTxnClient(clientId, hosts, &done, work_load, resultsCh)
			}(clientId, hosts)
		}
	}

	finish := func() {
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
		os.Exit(0)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			// sig is a ^C, handle it
			finish()
		}
	}()

	if *secs > 0 {
		time.Sleep(time.Duration(*secs) * time.Second)
		finish()
	} else {
		for {
		}
	}
}

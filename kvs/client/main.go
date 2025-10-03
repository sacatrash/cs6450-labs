package main

import (
	"flag"
	"fmt"
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
				//work_load := kvs.NewWorkload(*workload, *theta)
				work_load := kvs.NewAccountingWorkload(uint64(clientId), 10, 100, 50)
				runTxnClient(clientId, addrs, &done, work_load, resultsCh)
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

package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Stats struct {
	puts        *atomic.Uint64
	gets        *atomic.Uint64
	commits     *atomic.Uint64
	aborts      *atomic.Uint64
	abort_retry *atomic.Uint64
	abort_error *atomic.Uint64
}

func (s *Stats) Init() {
	s.puts = new(atomic.Uint64)
	s.gets = new(atomic.Uint64)
	s.commits = new(atomic.Uint64)
	s.aborts = new(atomic.Uint64)
	s.abort_error = new(atomic.Uint64)
	s.abort_retry = new(atomic.Uint64)
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.Init()
	r.puts.Store(s.puts.Load() - prev.puts.Load())
	r.gets.Store(s.gets.Load() - prev.gets.Load())
	r.commits.Store(s.commits.Load() - prev.commits.Load())
	r.aborts.Store(s.aborts.Load() - prev.aborts.Load())
	r.abort_error.Store(s.abort_error.Load() - prev.abort_error.Load())
	r.abort_retry.Store(s.abort_retry.Load() - prev.abort_retry.Load())
	return r
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
	kv.prevStats.abort_error.Store(stats.abort_error.Load())
	kv.prevStats.abort_retry.Store(stats.abort_retry.Load())
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
	abort_retry := diff.abort_retry.Load()
	abort_error := diff.abort_error.Load()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\ncommits/s %0.2f\naborts/s %0.2f\naborts/s (retries) %0.2f\naborts/s (error) %0.2f\n",
		float64(gets)/deltaS,
		float64(puts)/deltaS,
		float64(gets+puts)/deltaS,
		float64(commits)/deltaS,
		float64(aborts)/deltaS,
		float64(abort_retry)/deltaS,
		float64(abort_error)/deltaS)
}

package kvs

import "sync"

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

type Content struct {
	sync.Mutex
	Value string
}

/*
type LockRequest struct {
	locks []*sync.Mutex
	ret   chan int
}
*/

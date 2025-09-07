package kvs

import "sync"

type Content struct {
	sync.Mutex
	Value string
}

type LockRequest struct {
	locks []*sync.Mutex
	ret   chan int
}

// parent classes
type Request struct {
	Key  string
	Type string
	Ch   chan error //holds the return value
}

type Response struct{}

// inherited classes
type PutRequest struct {
	Request
	Key   string
	Value string
}

type PutResponse struct {
	Response
}

type GetRequest struct {
	Request
}

type GetResponse struct {
	Response
	Value string
}

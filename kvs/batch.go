package kvs

type Op struct {
	Key string
	Value string
	IsRead bool
}

type RequestBatch struct { Ops []Op}
type ResponseBatch struct { Values []string}
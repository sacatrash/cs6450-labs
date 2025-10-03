package kvs

type BatchOp struct {
	Callback chan any
	Op       *Op
}

type RequestBatch struct {
	Request
	Ops []Op
	Src string
}

type ResponseBatch struct {
	Response
	Values []string
}

func (r ResponseBatch) IsOk() bool {
	return r.Response.IsOk()
}

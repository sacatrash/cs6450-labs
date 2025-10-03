package kvs

type RequestBatch struct {
	Request
	Ops  []Op
	Dest string
	Src  string
}

type ResponseBatch struct {
	Response
	Values []string
	Src    string
	Dest   string
}

func (r ResponseBatch) IsOk() bool {
	return r.Response.IsOk()
}

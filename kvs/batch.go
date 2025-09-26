package kvs

type RequestBatch struct {
	Ops []Op
	Dest string
	Src string
}
type ResponseBatch struct {
	Values []string
	Src string
	Dest string
}
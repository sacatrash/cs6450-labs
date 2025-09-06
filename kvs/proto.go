package kvs

//parent classes
type Request struct {
	Key string
	Type string
	Ch chan error //holds the return value
}

type Response struct { }

//inherited classes
type PutRequest struct {
	Request
	Value string
	Type := "put"
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
	Type := "get"
}
package kvs

//parent classes
type Request struct {
	Key string
}

type Response struct { }

//inherited classes
type PutRequest struct {
	Request
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

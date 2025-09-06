package kvs

import "sync"

//queue corresponding to a node
type Queue struct {
	requests  []*RequestBatch
	responses []*ResponseBatch
	channels  []chan error
	nextNode  *Queue
	name      string
}

func (n Queue) AddTask(request *RequestBatch, response *ResponseBatch) *chan error {
	tmp := make(chan error)
	n.channels = append(n.channels, tmp)
	n.requests = append(n.requests, request)
	n.responses = append(n.responses, response)
	return &tmp
}

func (n Queue) next() *Queue {
	if n.nextNode == nil {
		return &n
	}
	return n.nextNode
}

func (n Queue) addNew(name string) *Queue {
	tmp := new(Queue)
	tmp.name = name
	tmpNext := n.nextNode
	n.nextNode = tmp
	tmp.nextNode = tmpNext
	if tmpNext == nil || tmpNext == tmpNext.nextNode {
		tmpNext = &n
	}
	return tmp
}

//masterQueue manages all queues under it.
//One queue per node.
//Rotate the actively processed node depending on a priority
//similar to a process scheduler (but I forgot the actual name)
type MasterQueue struct {
	Node2queue  map[string]*Queue
	format      string
	nodeList    []string
	locker      *sync.Mutex
	currentNode *Queue
	TargetMap   *map[string]string
	MaxElements int
}

func (m *MasterQueue) Initialize(lock *sync.Mutex, target *map[string]string) {
	m.Node2queue = make(map[string]*Queue)
	m.format = "RoundRobin"
	m.TargetMap = target
	m.locker = lock
	m.MaxElements = 10
}

func (m *MasterQueue) AddNewQueue(name string) *Queue {
	m.nodeList = append(m.nodeList, name)
	m.Node2queue[name] = m.Node2queue[m.nodeList[len(m.nodeList)-1]].addNew(name)
	return m.Node2queue[name]
}

func (m *MasterQueue) Process() {
	m.currentNode = m.currentNode.next()
	m.locker.Lock()
	defer m.locker.Unlock()
	m.currentNode.process(m.MaxElements, m.TargetMap)
}

//processes elements in the queue, and updates priorities
func (q *Queue) process(maxElements int, mp *map[string]string) {
	for i := 0; i < maxElements && len(q.channels) > 0; i++ {
		req := q.requests[1:][0]
		ch := q.channels[1:][0]
		res := q.responses[1:][0]
		for i, op := range req.Ops {
			if op.IsRead {
				if v, ok := (*mp)[op.Key]; ok {
					res.Values[i] = v
				}

			} else {
				(*mp)[op.Key] = op.Value
			}
		}
		ch <- nil
	}
}

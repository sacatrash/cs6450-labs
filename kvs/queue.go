package kvs

import (
	"container/list"
	"sync"
)

type Task struct {
	channels  chan error
	requests  *RequestBatch
	responses *ResponseBatch
}

// queue corresponding to a node
type Queue struct {
	sync.Mutex
	tasks    *list.List
	nextNode *Queue
	name     string
}

func (n *Queue) AddTask(request *RequestBatch, response *ResponseBatch) *chan error {
	tmp := make(chan error)
	tsk := Task{}
	tsk.channels = tmp
	tsk.requests = request
	tsk.responses = response
	n.tasks.PushBack(tsk)
	return &tmp
}

func (n *Queue) next() *Queue {
	if n.nextNode == nil {
		return n
	}
	return n.nextNode
}

func (n *Queue) addNew(name string) *Queue {
	n.Lock()
	defer n.Unlock()
	tmp := new(Queue)
	tmp.name = name
	tmpNext := n.nextNode
	n.nextNode = tmp
	tmp.nextNode = tmpNext
	if tmpNext == nil || tmpNext == tmpNext.nextNode {
		tmpNext = n
	}
	return tmp
}

// masterQueue manages all queues under it.
// One queue per node.
// Rotate the actively processed node depending on a priority
// similar to a process scheduler (but I forgot the actual name)
type MasterQueue struct {
	sync.Mutex
	Node2queue  map[string]*Queue
	format      string
	nodeList    []string
	currentNode *Queue
	TargetMap   *map[string]*Content
	MaxElements int
	lockList    *list.List
}

func (m *MasterQueue) Initialize(lockList *list.List, target *map[string]*Content) {
	m.Lock()
	defer m.Unlock()
	m.Node2queue = make(map[string]*Queue)
	m.format = "RoundRobin"
	m.TargetMap = target
	m.MaxElements = 10
	m.lockList = lockList
}

func (m *MasterQueue) AddNewQueue(name string) *Queue {
	m.Lock()
	defer m.Unlock()
	if len(m.nodeList) > 0 {
		m.Node2queue[name] = m.Node2queue[m.nodeList[len(m.nodeList)-1]].addNew(name)
	} else {
		m.Node2queue[name] = new(Queue)
		m.Node2queue[name].name = name
	}
	m.nodeList = append(m.nodeList, name)
	return m.Node2queue[name]
}

func (m *MasterQueue) Process() {
	m.Lock()
	if m.currentNode == nil {
		if len(m.nodeList) > 0 {
			m.currentNode = m.Node2queue[m.nodeList[0]]
		} else {
			return
		}
	} else {
		m.currentNode = m.currentNode.next()
	}
	nodeProc := m.currentNode
	m.Unlock()

	if nodeProc != nil {
		nodeProc.process(m.MaxElements, m.TargetMap, &m.Mutex, m.lockList)
	}
}

// processes elements in the queue, and updates priorities
func (q *Queue) process(maxElements int, mp *map[string]*Content, writeMtx *sync.Mutex, lockMtx *sync.Mutex) {

	for i := 0; i < maxElements && q.tasks.Front() != nil; i++ {
		q.Lock()
		tsk := q.tasks.Remove(q.tasks.Front()).(Task)
		q.Unlock()
		req := tsk.requests
		ch := tsk.channels
		res := tsk.responses
		//first let's get a slice of all locks we want
		var l LockRequest
		l.ret = make(chan int)
		for _, op := range req.Ops {
			if v, ok := (*mp)[op.Key]; ok {
				l.locks = append(l.locks, &v.Mutex)
			} else if !op.IsRead {
				l.locks = append(l.locks, writeMtx)
			}
		}

		//next, grab all the locks we need:
		//queue our locks list to a master queue,
		//then receive a channel callback when we can
		//grab the locks

		lockMtx.Lock()
		for k := 0; k < len(l.locks); k++ {
			l.locks[k].Lock()
		}
		lockMtx.Unlock()

		for j, op := range req.Ops {
			if op.IsRead {
				if v, ok := (*mp)[op.Key]; ok {
					res.Values[j] = v.Value
				}

			} else {
				(*mp)[op.Key] = Content{}
			}
		}
		ch <- nil
		close(ch)
		for k := 0; k < len(l.locks); k++ {
			l.locks[k].Unlock()
		}
	}

}

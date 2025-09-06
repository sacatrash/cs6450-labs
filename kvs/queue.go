//queue corresponding to a node
type Queue struct {
	tasks := make([]Request, 0)
	nextNode := nil
	name := ""
}

func (n Queue) next () Queue {
	return n.nextNode
}

func (n Queue) new (name string) Queue {
	tmp := Queue{}
	tmpNext = n.nextNode
	n.nextNode = &tmp
	tmp.nextNode = tmpNext
	if(tmpNext == nil) {
		tmpNext = n
	}
}

//masterQueue manages all queues under it.
//One queue per node.
//Rotate the actively processed node depending on a priority
//similar to a process scheduler (but I forgot the actual name)
type MasterQueue struct {
	node2queue map[string]Queue
	fmt := "RoundRobin"
	nodeList string[]
}

func (m MasterQueue*) initialize () {
	node2queue = make(map[string]Queue)
	nodeList = {}
}

func (m MasterQueue*) addNewQueue () {
	nodeList
}

func (m MasterQueue*) process () {

}

//processes elements in the queue, and updates priorities
func (q Queue*) process (maxElements := 10) {
	for i := 0; i < maxElements && i < len(q.tasks) > 0 {
		proc := q.tasks[1:]

	}
}
//masterQueue manages all queues under it.
//One queue per node.
//Rotate the actively processed node depending on a priority
//similar to a process scheduler (but I forgot the actual name)
type MasterQueue struct {
	node2queue map[]
}

func (m MasterQueue*) initialize () {

}

//queue corresponding to a node
type Queue struct {
	tasks := []Request{}
}

func (q Queue*) process (maxElements := 5) {

}
# Architecture notes

* server stores a queue per node, and processes batches from each queue in round-robin style. Up to 10 batches may be processed at once.

* There are two types of locks: per-queue, and per-key per-queue is used when tasks are to be added/removed from queues. per-key is used when writing or reading to the master list

* operation runs as follows:
	* an RPC is received. The batch is added to an appropriate queue,concurrently FIFO style
	* On a separate thread for processing the queues, the queue's lock is obtained or fails
	* all locks for the next batch are queued together, and obtained together. If there is a new key, a master lock is also acquired
	* With all key locks and the queue lock obtained, the queue can process a batch without conflict, and release the queue and key locks once done

* guarantees against deadlock
	* a queue's lock is only obtained by a thread to retrieve a task. No more than one thread can process a queue at any given time.
	* A queue hold all the locks it needs for a given task. As the locks are queued together, worst-case all locks are released before being given to the queue, in cascading fashion
	* In the case that two queues process entirely different keys, they are allowed to run concurrently
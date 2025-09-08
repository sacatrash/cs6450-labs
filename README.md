1. Results [1 to 2 paragraphs]

    Final throughput numbers:
        node0 median 2572209 op/s
        node1 median 2812553 op/s

        total 5384762 op/s

    Observed via htop, running on node0, the 16 threads can be observed to utilize about 30% usage while running, while memory utilization on node0 is not significant. Network traffic on node0 constituted to about 9 GB data, with 8 GB transmitted to clients and 1 GB received. 

    Our approach should scale linearly with more nodes, as we designed the client-server architecture to shard, where clients send data to one server instead of to all servers. As long as if the nu                                                   mber of servers scales proportionally with clients, we believe a proportional performance increase should be observed. 

Performance Grading Scale (YCSB-B, θ = 0.99) (only one component of the scoring rubric):

    80% grade: ≥ 400,000 op/s
    82% grade: ≥ 800,000 op/s
    88% grade: ≥ 1,600,000 op/s
    92% grade: ≥ 3,200,000 op/s
    95% grade: ≥ 6,400,000 op/s
    100% grade: ≥ 12,800,000 op/s

2. Design [3 to 4 paragraphs]

    Our current server-client architecture utilizes a 4096 element buffer to batch operations into grouped RPC calls. The buffer is flushed upon either filling up, or after a specified amount of time has passed. We tweaked the time vs. buffer capacity to maximize data getting sent but not to be significantly delayed by long operations.

    We increased the flush interval "ttlFlush" to 20 ms so batches fill up before sending. Previously we flushed too often, creating small batches and lower throughput. Larger batches cut RPC overhead and improved throughput.We also added async sending with double buffering. One buffer sends while the buffer fills. This avoids client idle time and keeps requests flowing without waiting on network round trips. 

    On the server, we replaced the global lock with Go’s sync.Map so most reads and updates can happen at the same time rather than waiting in line. We only take a lock when adding a brand-new key. Reads and updates to existing keys run in parallel and stay fast.

    The system design incorporates sharding across the servers. Each client uses a simple hash function on the operation’s key to determine which server is responsible for handling it. This ensures that requests are distributed evenly and prevents any single server from becoming overloaded. By spreading load across shards, the architecture achieves higher aggregate throughput, since multiple servers can process operations in parallel.              
    

3. Reproducibility [a few clear steps]

    Hardware setup:
    Configure and run `run-cluster.sh`. Our configuration requires no special OS level changes and can be run on a small-lan cluster in cloudlab.

4. Reflections [1 to 4 paragraphs]

   We learned a lot about the how RPC works, and about techniques which can be used to optimize workloads in a distributed system sense. We also learned about go, and the challenges of debugging and testing across a cluster of systems. I believe we can improve next time with better tools to profile performance, specifically recording results as we iterate to compare improvements and setbacks. People on the class discord server helped us with clarifications on the assignment requirements and with ideas on where to begin optimization.
 
    
    The first thing we tried was integrating a 64 byte batch buffer to group operations and reduce the number of locks/unlocks. This resultedin about 400k-800k ops/s, up to 1 mil. By adjusting the buffer to up to 4096 elements, we are now able to achieve around 2 mil.We then replaced the stats with atomic types, removing the need to lock whenever updating the stats. We later applied this changeto the map, using go's sync.map structure. This change likely improved our results by about 20k, though we didn't precisely compare.
    
    One approach that did not work as well as expected was using very short flush intervals. While this minimized latency, it caused the client to send many small batches, which significantly reduced throughput.

    There were some other things we tried but ended up abandoning as they didn't yield signfificant benefit. We tried to improve upon the batching structure by implementing a queue on the server, which would manually schedule processing RPCs with the idea that we may be able to achieve a higher average ops/s across all nodes by distributing the workload. While sound in theory, in practice we were only able to accomplish a single-threaded queue. If we were to multithread it, we would have run into complications with locks between the different queues and the different batches to run.

    Another thing we attempted was getting around HTTP being used by RPC, by utilizing gRPC. While a solid protocol and simple to implement on top of our existing code, the end result saw lower ops/s by about 40k. We think that, given the local environment that the nodes run, as well as the type of data being processed, gRPC/protobuf was less ideal for handling batches of data over the network.

    For further improvements, it may be interesting to look more into gRPC and see if HTTP/2 or /3 can work for an improvement. Additionally, the queuing system could evolve into a transactional architecture, and with multithreading support could improve performance while opening functionality for server-server replication to occur.

    Andy Herbert: I set up the initial cloudlab experiment, and looked into the queuing and gRPC methods of optimization which ended up not working. I also looked into and implemented the atomic datatypes, and improved batch handling by profiling runs with different sizes to find an ideal buffer size. Finally, Andy profiled runs to record the network and CPU utilization recorded in the readme.

    Jose Alex Garcia: I contributed primarily to the client side design and performance improvements. This included implementing the batching strategy, adjusting the flush interval to improve throughput, and helping integrate asynchronous sending with double-buffering. I also worked on analyzing the effect of sharding across servers and documented how these design changes impacted overall performance.

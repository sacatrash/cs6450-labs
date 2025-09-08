readme
Required README.md Sections
1. Results [1 to 2 paragraphs]

    Final throughput numbers:
        node0 median 2572209 op/s
        node1 median 2812553 op/s

        total 5384762 op/s

    Observed via htop, running on node0, the 16 threads can be observed to utilize about 30% usage while running, while memory utilization on node0 is not significant. Network traffic on node0 constituted to about 9 GB data, with 8 GB transmitted to clients and 1 GB received. 

    Our approach should scale linearly with more nodes, as we designed the client-server architecture to shard, where clients send data to one server instead of to all servers. As long as if the number of servers scales proportionally with clients, we believe a proportional performance increase should be observed. 

Performance Grading Scale (YCSB-B, θ = 0.99) (only one component of the scoring rubric):

    80% grade: ≥ 400,000 op/s
    82% grade: ≥ 800,000 op/s
    88% grade: ≥ 1,600,000 op/s
    92% grade: ≥ 3,200,000 op/s
    95% grade: ≥ 6,400,000 op/s
    100% grade: ≥ 12,800,000 op/s

2. Design [3 to 4 paragraphs]

    Our current server-client architecture utilizes a 4096 element buffer to batch operations into grouped RPC calls. The buffer is flushed upon either filling up, or after a specified amount of time has passed. We tweaked the time vs. buffer capacity to maximize data getting sent but not to be significantly delayed by long operations, ending with 20 ms ttl.

    Server-side, the key-value store is stored with an atomic map, eliminating the need for a master key for reads and writes. The master key is still used when a new value is being added into the data.

    

3. Reproducibility [a few clear steps]

    Hardware setup:         

4. Reflections [1 to 4 paragraphs]

    The first thing we tried was integrating a 64 byte batch buffer to group operations and reduce the number of locks/unlocks. This resultedin about 400k-800k ops/s, up to 1 mil. By adjusting the buffer to up to 4096 elements, we are now able to achieve around 2 mil.We then replaced the stats with atomic types, removing the need to lock whenever updating the stats. We later applied this changeto the map, using go's sync.map structure. This change likely improved our results by about 20k, though we didn't precisely compare.

    There were some other things we tried but ended up abandoning as they didn't yield signfificant benefit. We tried to improve upon the batching structure by implementing a queue on the server, which would manually schedule processing RPCs with the idea that we may be able to achieve a higher average ops/s across all nodes by distributing the workload. While sound in theory, in practice we were only able to accomplish a single-threaded queue. If we were to multithread it, we would have run into complications with locks between the different queues and the different batches to run.

    Another thing we attempted was getting around HTTP being used by RPC, by utilizing gRPC. While a solid protocol and simple to implement on top of our existing code, the end result saw lower ops/s by about 40k. We think that, given the local environment that the nodes run, as well as the type of data being processed, gRPC/protobuf was less ideal for handling batches of data over the network.

    What you learned from the assignment
    What optimizations worked well and why
    What didn't work and lessons learned
    Ideas for further improvement
    A short note on individual contributions from each team member

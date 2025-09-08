readme
Required README.md Sections
1. Results [1 to 2 paragraphs]

    Final throughput numbers:
        node0 median 2374966 op/s
        node1 median 2595395 op/s

        total 4970361 op/s

    Some rough numbers on hardware utilization metrics (CPU, memory, network)

    Scaling characteristics (how performance changes with cluster size and/or with increasing offered client load)
        At a minimum, if your approach scales run it with small scale and larger scale
    Any performance graphs and visualizations for the above

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

    Step-by-step instructions to reproduce results
    Hardware requirements and setup
    Software dependencies and installation if anything more than go, etc
    Configuration parameters and their effects in particular if you've added "knobs"

4. Reflections [1 to 4 paragraphs]

    The first thing we tried was integrating a 64 byte batch buffer to group operations and reduce the number of locks/unlocks. This resultedin about 400k-800k ops/s, up to 1 mil. By adjusting the buffer to up to 4096 elements, we are now able to achieve around 2 mil.We then replaced the stats with atomic types, removing the need to lock whenever updating the stats. We later applied this changeto the map, using go's sync.map structure. This change likely improved our results by about 20k, though we didn't precisely compare.

    There were some other things we tried but ended up abandoning as they didn't yield signfificant benefit.

    What you learned from the assignment
    What optimizations worked well and why
    What didn't work and lessons learned
    Ideas for further improvement
    A short note on individual contributions from each team member

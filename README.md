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

    Changes you made to the design and what effect they had on performance
    A rationale for these design choices
    Trade-offs and design alternatives considered
    Any performance bottleneck analyses you did to arrive at the above conclusions

3. Reproducibility [a few clear steps]

    Step-by-step instructions to reproduce results
    Hardware requirements and setup
    Software dependencies and installation if anything more than go, etc
    Configuration parameters and their effects in particular if you've added "knobs"

4. Reflections [1 to 4 paragraphs]

    What you learned from the assignment
    What optimizations worked well and why
    What didn't work and lessons learned
    Ideas for further improvement
    A short note on individual contributions from each team member

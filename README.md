readme
Required README.md Sections
1. Results [1 to 2 paragraphs]

    Final throughput numbers:
        Serializability check passes.

        W/ -generators=100

        node0 median ~500 op/s
        node1 median ~500 op/s

        total ~1000 op/s

        We also observe about 250-300 commits/s and 10-15 aborts/s.

    With strict serializability checking, performance is noticably low compared to assignment 1, which makes sense as we are no longer batching, but also aborts reduce the ops. This is with the default theta of .99.

    When theta=0, we observe a total throughput of around 700 ops/s, with zero aborts. With theta=.5, throughput is about 750 ops/s with again 0 aborts. At theta=.8, observed results yield around 710 ops/s and less than 5 aborts/s. With theta=1, node0 median reportely is around 900 ops/s, node1 26300, with a total about 27200 ops/s. The commit to abort ratio varied widely per second.

    We tested by implementing Visual Studio Code tasks to assist with smaller scale debugging, then ran the sh file for multi machine tests. Serializability was tested with the bank transfer workload (Accounting), with assertions printed if an incorrect balance was detected at random.

2. Design [3 to 4 paragraphs]

    We overhauled a lot of the prior server and client architecture to make it simpler and more modular. Structurally, the numerous structs for different types of requests and responses were simplified into a few depending on what the RPC actually needed. Client code was adapted into interfaces to allow for PA1 to be run alongside PA2 in theory (granted PA1 was updated to the newer codebase). The workload now calls RPCs directly, with the option of running them asychronously, as each RPC returns a channel which writes response value. Client connections were moved prior to workload generation, so that only one connection to each host had to be established instead of one connection per generator.

    The transactional model uses a 2PL/2PC method with proven strict serializability. We use a shared read lock and a single write lock on each key. There is a flag in the code which can vary the behavior of handling write lock attempts when read locks are present:

    0=no special handling. If no write lock is present, the transaction acquires the write lock
    1=abort write. If any read lock is present other than the transaction's own lock, the write aborts
    2=abort reads. If any read lock is present other than the transaction's own lock, the reads abort
    3=snapshot. Upon calling begin, a snapshot of all keys is created which a txid would read from. All reads read from the snapshot, or previously recorded written values of the transaction. If there is a write, the write is aborted if the current committed value does not match the snapshot's value.

    0 obviously breaks serializability. In testing, 3 also breaks serializability, though we believe in theory it shouldn't, so there may be an issue with how this was implemented. 2 and 3 maintain strict serializibility at the cost of more retry aborts, with no discernable difference with the accounting workload. 2 yields the worst ops/s.

    Client side retry was also implemented, where the client can retry an RPC after a delay upon receiving a lock error. This did not yield any improvement so it's currently set to only delay upon a lock error, with no retry.

    In deadlocks.md is an outline of a potential timestamp system that was discussed but not implemented. The idea of this system to not depend heavily on sychronized clocks, was to queue transactions and commit the earliest recorded one, aborting transactions if they showed older than the oldest or if they were queued for too long as the last transaction. 

3. Reproducibility [a few clear steps]

    Hardware setup:
    Configure and run `run-cluster.sh`. Our configuration requires no special OS level changes and can be run on a small-lan cluster in cloudlab.

    It is recommended to set the host_generators argument for clients to a value between 100-500, as there is available capacity observed on clients for more workload generation. This may be suggestive instead of an unkown bottleneck.

    The transfer workload can be passed with the Accounting workload parameter. When done, parameters related to the other workload are ignored.

4. Reflections [1 to 4 paragraphs]

   

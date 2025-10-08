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

    On the client side, TxnClient manages the full transaction flow: Begin, three operations, commit or abort. The begin, commit and abort requests are sent to all shards with one marked as Lead=true so servers know which one to count the commit from. Each individual Get or Put is sent only to the shard that owns the corresponding key. Our goal is to keep this straight forward where each server handles locking and staginf of its own writes, and two phase commit ensures that all shards apply their updates atomically.

    Each client starts a transaction with a TxBegin on all shards then issues exactly three ops (Get / Put) to the shards that own the keys and finally ends with TxCommit or TxAbort on all shards. The reads and writes are per op locked via shared (S lock) and exclusive (X lock) locks on the server. The reads first attempt to take an S lock whereas writes try to take an X lock. If a lock cannot be granted, the server returns a lock fail error and the client aborts and retries the same 3 ops. Commits apply the staged write set and release all locks. Aborts drop the staged state and release locks. We count only the leader shard's commit or abort for throughput. 

    On the server, every key has a lock record with a set of reader TxIDs (S holders) and an optional writer TxID (x holder). Every transaction has a txState that tracks its staged write set, and the sets of S/X locks it holds so that TxCommit/TxAbort can call one releaseAll(txid) to atomically drop everything. Lock acquisition is non blocking(tryAcquireS/tryAcquireX). If a lock conflict occurs, the server immediately returns an error, allowing the client to abort and retry the same three operations exactly as before. 

    The transactional model uses a 2PL/2PC method with proven strict serializability. We use a shared read lock and a single write lock on each key. There is a flag in the code which can vary the behavior of handling write lock attempts when read locks are present:

    0=no special handling. If no write lock is present, the transaction acquires the write lock
    1=abort write. If any read lock is present other than the transaction's own lock, the write aborts
    2=abort reads. If any read lock is present other than the transaction's own lock, the reads abort
    3=snapshot. Upon calling begin, a snapshot of all keys is created which a txid would read from. All reads read from the snapshot, or previously recorded written values of the transaction. If there is a write, the write is aborted if the current committed value does not match the snapshot's value.

    0 obviously breaks serializability. In testing, 3 also breaks serializability, though we believe in theory it shouldn't, so there may be an issue with how this was implemented. 2 and 3 maintain strict serializibility at the cost of more retry aborts, with no discernable difference with the accounting workload. 2 yields the worst ops/s.

    All reported results were obtained using mode 1 (abort writer/ no wait for X) as the default configuration. Mode 1 maintins serializability while preventing reader starvation in read heavy workloads. Mode 2 (abort readers) is also serializable but resulted in higher overhead under YCSB-B so we kept it available for comparisons. 

    Client side retry was also implemented, where the client can retry an RPC after a delay upon receiving a lock error. This did not yield any improvement so it's currently set to only delay upon a lock error, with no retry.

    Our deadlock policy is to avoid waits entirely. We did this to prevent distributed deadlock complexity at the cost of more aborts under contention. To reduce convoying, the client introduces a small randomized backoff after an abort before retrying the same three operations. This improved stability on skewed YCSB-B workloads without slowing down successful transactions.

    In deadlocks.md is an outline of a potential timestamp system that was discussed but not implemented. The idea of this system to not depend heavily on sychronized clocks, was to queue transactions and commit the earliest recorded one, aborting transactions if they showed older than the oldest or if they were queued for too long as the last transaction. 

    Finally, we used the accounting workload as a built in serializability check. Each client transfers 100$ from a source account to a destination account in three operations and periodically runs a seperate sum transaction that reads all ten accounts to verify the total remains $10k. Since reads acquire shared S locks and writes require exclusive X locks, any lost update, read, skew, or cross shard misordering would eventually break this invariant. 

3. Reproducibility [a few clear steps]

    Hardware setup:
    Configure and run `run-cluster.sh`. Our configuration requires no special OS level changes and can be run on a small-lan cluster in cloudlab.

    It is recommended to set the host_generators argument for clients to a value between 100-500, as there is available capacity observed on clients for more workload generation. This may be suggestive instead of an unkown bottleneck.

    The transfer workload can be passed with the Accounting workload parameter. When done, parameters related to the other workload are ignored.

4. Reflections [1 to 4 paragraphs]

    We learned about the challenges around transactional systems and serializability. There were many systems which on paper may appear to be simple solutions, like timestamps or snapshotting, yet in practice reveal issues and complications that could be difficult to anticipate.

    what worked well was keeping to a straightforward 2PL+2PC design with per key shared / exclusive locks and a minimal transactional API (begin->three ops-> commit or abort). Routing each operations by shard simplified the client while the server's staged write set + releaseAll(txid) pattern made commit and abort behaviour easier to reason about. Using a VS Code compound setup shortened our debug cycle and leader only commit accounting prevented double counting in 2PC. These choices helped us maintain stable throughput under load. 

    Pushing transaction control into the workload introduced unwanted coupling and made it hard to follow the assignment's retry the same 3 ops as is rule. Because of this, we moved that logic back into the client/server layer. 

    For future improvements, we would pre decalre the three keys for each transaction, determine the strongest required lock mode and pre lock them per shard in a consistent global order to perhaps reduce aborts. We could also explore waiting schemes if they improve stability compared to our no wait approach. 

    Alex Garcia implemented the TxnClient orchestration which handles the transaction flow (begin on all shards-> 3 ops-> commit or abort on all shards). This included per operation routing by shard and designing a single leader shard to ensure accurate commit and abort reporting. On the server side, Alex ensured that every transactions RPC carries a TxID and added the phase 2 handler so each shard can apply or discard its staged state. Alex also designed and integrated the lock checking logic used by the servers. Shared S locks for reads and exclusive X locks for writes with a rule for handling cases where a write attempts to acquire an X lock while other transactions already hold S locks on the same key. This write lock resolve rule is configurable and it does not impact S vs S interactions. Reads first check the transactions staged write set before falling back to the committed data map. At commit, the shard installs all staged values and calls releaseAll(txid) to release every s/x lock held by the transaction. On abort, it discards the staged values and calls the same function to ensure a clean rollback.

    Andy Herbert worked considerably on refactoring the prior codebase to improve and streamline functionality. He also fixed multiple issues with the first implementation of the code, revising the transaction model to work with the serializability test in the assignment and making the workloads more directly control operations. Finally, he performed testing and adjustment to ensure serializability and attempt to increase performance.
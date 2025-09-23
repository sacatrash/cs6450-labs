# Deadlocks

## Scenario:

* multiple servers
* each server contains its own set of keys
* commits are staged in the form of batches- each server holds its own unique set of commits

Assume this structure:
* client sends batches all out at once, waits for response
* if response good, client sends commit; if bad, client aborts
* upon committing, all changes are written/read

## Structure 1: sychronous transactions

Locks: sorted in order. At least per machine, there would be no deadlocking.

A basic structure: one transaction at a time

A complication: two transactions are sent at same time, received differently by different servers

* Server receives batches with a timestamp; places batches in order, and responds with success if batch within server's processing timeframe
* the processing timeframe, synchronized between servers, indicates the period which to collect rpc timestamps. If outside the range, batch is dropped.
* Upon commit: if the commit is the earliest unprocessed rpc, process it via goroutine and move the timeframe up to its time
* if the commit is not the earliest, do not process; queue for processing
* if commit is beyond the timeframe's max range, drop/abort

During processing:
* 2PL gather all locks, release when done

Guarantees:
* commits only occur if all servers have received all batches for commit
* locks are acquired in time order, and only once a commit is being processed.
* assume locks are sorted and obtained in a distinct order
* because of the above guarantee, commits can be processed in parallel if they do not depend on another's locks being held
* if any lock is locked, the commit will get the lock at the same order as the other servers, in the order by timestamp.
* Only one timestamp may write to a key at any time.
# ParallelMassive Working Notes

This file is the persistent handoff and change tracker for this repository. Read it before modifying the mining code. Update the **Current state**, **Validation**, **Known issues**, and **Change log** sections whenever a material implementation, configuration, correctness, memory, or performance change is made.

## Project objective

Develop and evaluate a PTM-style top-k high-utility itemset miner for massive databases. The current experimental contribution is candidate-level parallel mining with early threshold raising, SU-prioritized scheduling, bounded resident work, and a true serial DFS baseline.

Correctness has priority over speed. Any parallel change must be compared against the independent serial TKEH verifier before it is treated as valid.

## Important files

- `src/AlgoEFIM_PTMStyleBaseline.java`: main PTM-style serial/parallel implementation.
- `src/AlgoTKEHSerialVerifier.java`: independent serial TKEH correctness oracle.
- `src/testCorrectnessParallelVsSerial.java`: compares TKEH oracle, PTM serial DFS, and PTM parallel result sets.
- `src/testEFIM_PTMChunk.java`: main experiment runner; redirects output to a timestamped log file.
- `src/testTKEHSerial.java`: serial verifier runner and result logger.
- `src/Transaction.java`: transaction and projected-transaction representation.

## Current algorithm state

### Shared preprocessing

1. Scan the input for TWU, support, singleton utility, and optional transaction-utility witnesses.
2. Apply RIU using exact singleton utilities.
3. Optionally raise the safe lower bound from distinct transaction witnesses.
4. Rename promising items.
5. Build PTM prefix partitions by one reduced-database pass.
6. Order partitions by average suffix transaction utility.
7. Load one partition fully into RAM.
8. Compute root-pair exact utility, LU, and SU with reusable one-dimensional workspaces. There is no global `n²` pair matrix and no extra all-partition pair scan.

### Serial mode

`candidateParallelism=false` selects genuine recursive DFS:

- no candidate scheduler;
- no worker threads;
- no shared candidate frontier;
- siblings may be ordered by descending SU when `bestSUFirst=true`;
- Direct-U and the same LU/SU pruning logic remain available.

Entry points:

- `minePartitionDepthFirst(...)`
- `mineCandidateDepthFirst(...)`

### Parallel mode

`candidateParallelism=true` selects the custom candidate scheduler.

The scheduler no longer uses `ThreadPoolExecutor` and does not call `submit()` or `execute()` for each candidate. It contains:

- a fixed array of long-lived worker threads;
- one shared `PriorityBlockingQueue<CandidateTask>`;
- descending-SU ordering when `bestSUFirst=true`;
- FIFO sequence tie/order when Best-SU is disabled;
- a `Semaphore` for the default fixed-limit mode, or optional adaptive heap-pressure admission;
- worker loops that call `candidateQueue.take()` directly.

`maximumOutstandingCandidateTasks=0` selects adaptive admission. The shared frontier grows while Java heap usage is below 72%, switches overflow candidates to synchronous inline execution at or above 72%, and resumes asynchronous growth only after usage falls to 60%. At least one queued task per worker is still admitted so heap pressure cannot remove all shared parallel slack. The 72%/60% hysteresis avoids rapid mode oscillation and does not force GC.

A positive `maximumOutstandingCandidateTasks` retains the fixed-limit mode for controlled experiments. With 8 workers and a positive limit of 1024, the intended bound is approximately 1024 queued tasks plus 8 running tasks. The Java priority queue itself is unbounded; admission control is what bounds or throttles its resident work.

Candidate creation rule:

- acquire a fixed queue slot or adaptive admission first;
- only then allocate `CandidateTask` and enqueue it;
- if no slot is available, process the candidate synchronously using method parameters, without allocating a task and without incrementing asynchronous `pending` work.
- `minimumTransactionsPerCandidateTask` provides cost-based granularity. If a candidate's parent projection has fewer transactions than the threshold, the entire candidate subtree switches to genuine `mineCandidateDepthFirst(...)`; descendant projections cannot grow, so no descendant in that subtree would become task-worthy later.

Initial candidates follow the same rule and are no longer materialized as a full `List<CandidateTask>`.

### Threshold raising and ordering

- `directUtilityRaising=true` computes exact utilities of all immediate children in the same suffix scan used for LU/SU and offers them to global top-k before scheduling the children.
- Exact utility raises the threshold; it is not the queue priority.
- `bestSUFirst=true` uses SU as the candidate priority because SU is the available upper bound on the candidate subtree.
- `bestChildContinuation=true` processes the greatest-SU child immediately on the current worker; siblings are returned to the shared priority frontier.
- `topKThresholdCertified` becomes true when either the exact heap contains `k` patterns or `k` safe singleton/transaction witnesses certify a lower bound.
- `thresholdReadyParallelism=true` performs serial DFS warm-up until the threshold is certified, then uses the pool on later partitions.
- For very large `k`, the principal experiment should also test `thresholdReadyParallelism=false`, because waiting for certification can turn most of the run into DFS.

### Shared top-k correctness

- `minUtil` is `volatile` and never decreases.
- All `PriorityQueue<TopKPattern>` mutations are inside `synchronized (topKQueue)`.
- `offerTopK(...)` rejects `utility < minUtil` before acquiring the heap lock, then rechecks after acquiring it.
- Direct-U must not insert the same candidate again when its task executes; this is controlled by `utilityAlreadyOffered`.

### Current experiment switches

In `testEFIM_PTMChunk.java`:

```java
boolean candidateParallelism = true;
boolean directUtilityRaising = true;
boolean bestSUFirst = true;
boolean transactionUtilityRaising = true;
boolean thresholdReadyParallelism = true;
boolean bestChildContinuation = true;
int maximumOutstandingCandidateTasks = 1024;
int candidateWorkers = 8;
```

For immediate parallelism on very large `k`, change only:

```java
boolean thresholdReadyParallelism = false;
```

## Concurrency invariants

Do not violate these rules:

1. Increment `PartitionRun.pending` before publishing a task to the shared queue.
2. Every queued task must call `finishOne()` exactly once.
3. A synchronous inline candidate is part of its caller's active work and must not independently increment/decrement `pending`.
4. Release fixed-mode semaphore admission and decrement queued-task admission as soon as a worker removes the corresponding task from the queue.
5. Never enqueue a task without first acquiring fixed or adaptive queue admission.
6. Snapshot all child SU values before running any child inline because `WorkerBins` is thread-local but reused and reset recursively.
7. A worker must not mutate a parent projected database that can be referenced by sibling tasks. Transaction merging must copy on the first merge before later in-place accumulation.
8. Do not block all workers while attempting to put children into a full bounded queue; that can deadlock. Queue overflow must use synchronous continuation or another nonblocking, correctness-preserving path.
9. Do not replace `volatile minUtil` with a non-visible ordinary field.
10. Do not access or mutate the global top-k heap concurrently outside its lock.
11. Cost-based DFS fallback must use the existing `utilityAlreadyOffered` value so Direct-U never inserts a candidate twice.

## Performance instrumentation policy

Hot-path statistics were intentionally removed because they materially distorted parallel performance. Do not restore per-candidate counters in production benchmark runs.

Removed instrumentation includes:

- candidate count;
- projected transaction-read count;
- merge count;
- candidate RAM-node count;
- read/write byte counters;
- per-worker statistic accumulation;
- per-partition `MemoryLogger.checkMemory()`;
- per-partition progress and `[LOAD RAM]` logging.

The remaining statistics are low-frequency configuration/result fields, total elapsed time, final threshold, and DFS/pool partition counts.

If detailed counters are needed for diagnosis, put them behind an explicit disabled-by-default diagnostic flag and do not compare a diagnostic run against a production run.

`configureDiagnosticStatistics(true)` currently enables `LongAdder` counters for processed candidates, asynchronously enqueued tasks, whole-subtree small-DFS switches, and queue-overflow inline executions. The main runner may enable it temporarily for diagnosis; turn it off before any production timing comparison.

## Validation

Compile all sources:

```powershell
javac -encoding UTF-8 -d out src/*.java
```

Published TKEH example, fully parallel from the start:

```powershell
java -cp out testCorrectnessParallelVsSerial dataset/tkeh_example.txt 10 4 2147483647 true true 8 false true
```

Chainstore prefix, fully parallel from the start:

```powershell
java -cp out testCorrectnessParallelVsSerial dataset/chainstore.txt 1000 8 100 true true 128 false true
```

Arguments for `testCorrectnessParallelVsSerial`:

1. input path
2. k
3. worker count
4. maximum transaction count
5. Direct-U
6. Best-SU-first
7. maximum outstanding tasks
8. threshold-ready parallelism
9. best-child continuation
10. minimum parent transactions required for an asynchronous candidate task

Validated results as of 2026-09-09:

- Published TKEH Top-10: oracle = PTM DFS = PTM pool; Table 11 check passes.
- Chainstore first 100 transactions, `k=1000`: oracle = PTM DFS = fully parallel pool; final `minUtil=11530`.
- Queue-overflow/inline path was exercised with a queue limit of 8 and passed the published example.
- Earlier chainstore first 50 transactions, `k=500`, with Direct-U and Best-SU both disabled also matched the oracle before the scheduler rewrite.
- Adaptive admission smoke test passed against both the TKEH oracle and PTM DFS on a five-transaction synthetic database: `java -cp out testCorrectnessParallelVsSerial /private/tmp/itp_adaptive_correctness.txt 10 4 2147483647 true true 0 false true`; all modes returned the same 10 patterns with final `minUtil=17`.
- Fixed admission regression on the same database also passed: `java -cp out testCorrectnessParallelVsSerial /private/tmp/itp_adaptive_correctness.txt 10 4 2147483647 true true 8 false true`; oracle, DFS, and pool again returned the same 10 patterns with final `minUtil=17`.
- Exact restored-runner-mode smoke test passed: `java -cp out testCorrectnessParallelVsSerial /private/tmp/itp_adaptive_correctness.txt 10 4 2147483647 true true 8 true true`; oracle, DFS, and threshold-ready fixed pool returned the same 10 patterns with final `minUtil=17`.
- A discarded 64-entry worker top-k batching experiment remained correct but did not improve full-chainstore time: fixed-1024 pool changed from 307,545 ms to 308,824 ms under the user's identical flags. The batching code was removed.
- A discarded cross-partition window passed correctness on synthetic data but regressed full-chainstore time to 385,146 ms and caused 12,087 rather than 12,078 partitions to enter pool mining because concurrent preprocessing delayed threshold effects. It was removed.
- Cost-based whole-subtree DFS fallback passed the Chainstore first-100 oracle check at threshold 512: `java -cp out testCorrectnessParallelVsSerial dataset/chainstore.txt 1000 8 100 false false 128 false false 512`; oracle, DFS, and pool returned the same 1,000 patterns with final `minUtil=11530`.
- A full diagnostic run of whole-subtree DFS threshold 512 returned the correct one million patterns and `minUtil=91900`: 527,120,663 candidates, 55,615 asynchronous tasks, 20,963 small-subtree DFS switches, and zero queue-overflow executions. Its 306,144 ms elapsed time includes diagnostic counter overhead and is not a production timing result.
- The matching user-run diagnostic serial DFS processed 527,120,735 candidates in 303,911 ms with the same final `minUtil=91900`. The pool's 72-fewer candidates are expected from concurrent threshold-raising order and are negligible relative to 527 million candidates; diagnostic pool remained about 0.7% slower.
- The matching production pool run with diagnostics disabled and whole-subtree threshold 512 completed in 305,355 ms. This improved only about 0.7% over the earlier 307,545 ms pool and remained about 3.2% slower than the 295,917 ms production DFS run.
- Historical full-chainstore logs show the former fixed-1024 configuration completed the parallel run in 369,543 ms versus 500,788 ms for DFS. Threshold-ready warm-up accounted for only 1 DFS partition before 12,078 pool partitions, so it was not the material bottleneck in that run. These differently instrumented historical runs are diagnostic evidence, not a new performance claim.

## Benchmark cautions

- Do not infer speedup from different flags. Compare DFS and pool using identical Direct-U, Best-SU, transaction raising, merging, pruning, input, `k`, heap size, and transaction limit.
- Run multiple repetitions and alternate order. Partition directory deletion/build and Windows/OneDrive filesystem caching can heavily bias the first run.
- `startTimestamp` currently includes preprocessing, partition-directory cleanup, partition construction, mining, and final result construction.
- A small dataset may not amortize worker/queue startup.
- Equal final `minUtil` is necessary but not sufficient for correctness; compare canonical itemset-to-utility maps with the oracle.
- `PriorityBlockingQueue` is thread-safe. Memory growth is caused mainly by retained task/projected-database references and allocation rate, not by missing queue synchronization.
- A queued task retains its parent projected transaction list, `itemsToKeep`, and prefix. Increasing the outstanding limit can therefore increase live memory even though tasks do not copy the whole database.

## Known issues and next experiments

1. Full `chainstore`, very large `k` must be rerun after the custom worker-loop rewrite and instrumentation removal.
2. Compare outstanding limits 64, 128, 256, and 1024 with identical settings. Track elapsed time and external JVM peak RSS if memory counters remain disabled.
3. Compare `thresholdReadyParallelism=false` versus `true` for very large `k`.
4. Compare candidate pool with 1, 2, 4, and 8 workers. `candidateParallelism=false` is DFS, while `candidateParallelism=true, workers=1` is the scheduler baseline.
5. The global top-k heap remains a serialization point for one million results despite the fast rejection path.
6. `bestChildContinuation` uses recursion and can retain an ancestor projection chain; extremely long transactions may require an iterative local stack.
7. In fixed mode the custom queue is bounded logically by a semaphore rather than by the queue implementation. In adaptive mode it is throttled by heap-pressure hysteresis and queued-task accounting. Preserve and test both admission paths.
8. The partition directory name includes the JVM maximum-memory value and a literal `10`; this naming is awkward and should be cleaned only with care because existing experiment scripts may rely on it.
9. Adaptive queue admission currently uses Java-heap occupancy, not native memory or an estimate of retained projected-database graphs. Benchmark the 72% pause / 60% resume watermarks on full chainstore and compare them against fixed limits before treating them as tuned defaults.
10. Adaptive admission is retained as an experiment but is not the main runner default: on the user's large database it was reported substantially slower than serial. An unbounded sub-72% frontier can increase priority-queue traffic and retain too many projected-database graphs.
11. With `k=1,000,000`, Direct-U can offer a very large number of exact children before the result heap fills and raises `minUtil`. Parallel workers then serialize on the global top-k heap; measure this separately from candidate scheduling before redesigning the heap.
12. The fixed-1024 candidate pool took 308,824 ms versus 295,917 ms for recursive DFS with Direct-U, Best-SU, and best-child continuation all disabled. This points to fine-grained candidate scheduling/priority-queue overhead or insufficient work per partition; 64-entry top-k batching did not change the result materially.
13. Cross-partition concurrency was measured and rejected: it weakened early threshold effects and took 385,146 ms on full chainstore. Keep partitions sequential unless a future design preserves threshold order.
14. With Direct-U and Best-SU disabled, enabling best-child continuation regressed full-chainstore time from 308,824 ms to 407,931 ms. On this database the recursive strongest-child chain reduces useful parallelism; keep it disabled for the current comparison.
15. The first minimum-task implementation only ran each small node inline through the scheduler path. Thresholds 64 and 512 produced 305,156 ms and 307,781 ms respectively, so that version was insufficient. Whole-subtree DFS at 512 reduced async task creation to 55,615, but still processed 527,120,663 candidates; further speedup must reduce candidate/projection work or shared-memory contention, not merely queue operations.
16. With only 55,615 asynchronous tasks and zero queue-overflow events, task admission is no longer the dominant measured cost. Investigate projected-database memory bandwidth, workload balance, and shared top-k updates before raising the task threshold further.
17. The next user-run diagnostic isolates `bestSUFirst=true` with Direct-U and best-child continuation disabled. Compare its candidate count against 527,120,663; only run diagnostics-off timing if Best-SU materially reduces candidates.
18. Per user request, future agents must only run small demos or bounded dataset prefixes. The user will run full-database benchmarks and report results.

## Change log

### 2026-09-09

- Restored a genuine recursive serial DFS path for `candidateParallelism=false`.
- Kept candidate pool only for parallel mode.
- Added threshold-ready DFS-to-pool switching and threshold certification from exact patterns or safe witnesses.
- Added greatest-SU best-child continuation.
- Fixed the earlier inline-child `WorkerBins` corruption by snapshotting every child SU before any recursive/inline execution.
- Removed synchronized hot-path statistics and then removed production hot-path counters entirely.
- Removed per-partition progress, load, byte-I/O, and memory logging.
- Added fast rejection before the synchronized global top-k heap.
- Replaced per-candidate `ThreadPoolExecutor.execute()` with fixed worker loops over a shared priority queue.
- Changed task admission so queue-overflow candidates run inline without allocating `CandidateTask`.
- Removed bulk allocation of initial root `CandidateTask` objects.
- Extended correctness-test arguments for threshold-ready and best-child-continuation modes.
- Added adaptive candidate admission (`maximumOutstandingCandidateTasks=0`): the priority frontier grows below 72% Java-heap usage, falls back to the existing inline path under pressure, and resumes below 60%; retained positive fixed limits for comparison.
- Added admission mode to console and log-file configuration labels.
- Restored fixed `maximumOutstandingCandidateTasks=1024` as the main experiment-runner default after the adaptive frontier was reported substantially slower on the user's large database; adaptive mode remains available with value `0` for controlled experiments.
- Removed adaptive queued-task atomic accounting from the fixed-limit hot path, leaving fixed admission with the original semaphore acquire/release behavior.
- Added 64-entry per-thread top-k offer buffers for candidate-pool work. A full buffer updates the exact global heap under one lock, and all partial buffers flush at every pool-partition boundary; serial/root preprocessing remains immediate.
- Removed the worker top-k batching experiment after the full-chainstore result changed only from 307,545 ms to 308,824 ms. Keep the immediate heap update path until a stronger contention design is validated.
- Replaced the per-partition submit/wait barrier in candidate-pool mode with a bounded cross-partition window of up to one active partition per worker. Each partition retains independent pending/failure completion and releases its root database only after completion.
- Removed the cross-partition window after full-chainstore validation regressed to 385,146 ms; restored strict partition-by-partition threshold order and cleanup.
- Disabled best-child continuation in the main runner again after an identical full-chainstore run took 407,931 ms versus 308,824 ms without it.
- Added configurable candidate task granularity by parent projected-transaction count. Small candidates now switch their entire monotone-shrinking subtree to genuine DFS rather than merely executing one scheduler node inline.
- Added disabled-by-design diagnostic counters for candidates, asynchronous tasks, small-subtree DFS switches, and queue-overflow inline work; diagnostic timing must not be compared with production timing.

## Required maintenance for future agents

After every material change:

1. Update the relevant description in **Current algorithm state**.
2. Preserve or revise the **Concurrency invariants**.
3. Add the exact validation command and outcome to **Validation**.
4. Add unresolved risks or follow-up measurements to **Known issues and next experiments**.
5. Append a dated bullet to **Change log**.
6. Never claim a performance improvement from a single, differently configured, or cache-biased run.

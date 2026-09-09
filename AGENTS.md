# ParallelMassive Working Notes

This file is the persistent handoff and change tracker for this repository. Read it before modifying the mining code. Update the **Current state**, **Validation**, **Known issues**, and **Change log** sections whenever a material implementation, configuration, correctness, memory, or performance change is made.

## Project objective

Develop and evaluate a PTM-style top-k high-utility itemset miner. The current experimental contribution is candidate-level parallel mining with fixed transaction/global-pair threshold raising, work-aware SU scheduling, bounded resident work, and a true serial DFS baseline.

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

1. Discover the maximum item identifier, then scan the input for TWU, support, singleton utility, transaction-utility witnesses, and exact global pair utility.
2. Apply RIU using exact singleton utilities.
3. Offer every observed exact 2-itemset from the global `long[maxItem+1][maxItem+1]` matrix to top-k, then release the matrix before partition construction.
4. Raise the safe lower bound from distinct transaction witnesses.
5. Rename promising items.
6. Build PTM prefix partitions by one reduced-database pass.
7. Order partitions by average suffix transaction utility.
8. Load one partition fully into RAM.
9. Compute root-pair LU, SU, and estimated scan work with reusable one-dimensional workspaces. Exact pair utility is not recomputed or offered during partition mining.

### Serial mode

`candidateParallelism=false` selects genuine recursive DFS:

- no candidate scheduler;
- no worker threads;
- no shared candidate frontier;
- sibling order is unchanged by the pool-priority flag, providing a clean DFS baseline;
- exact candidate utility and LU/SU pruning remain active; Direct-U child pre-evaluation remains implemented but is disabled by default.

Entry points:

- `minePartitionDepthFirst(...)`
- `mineCandidateDepthFirst(...)`

### Parallel mode

`candidateParallelism=true` selects the custom candidate scheduler.

The scheduler no longer uses `ThreadPoolExecutor` and does not call `submit()` or `execute()` for each candidate. It contains:

- a fixed array of long-lived worker threads;
- one shared `PriorityBlockingQueue<CandidateTask>`;
- descending `SU / estimatedWork` ordering when `workAwarePriority=true`;
- FIFO sequence order when work-aware priority is disabled;
- a `Semaphore` that logically bounds resident queued tasks;
- worker loops that call `candidateQueue.take()` directly.

Candidate admission now has two modes:

- positive `maximumOutstandingCandidateTasks`: fixed semaphore-bounded frontier;
- zero `maximumOutstandingCandidateTasks`: experimental heap-pressure admission with one 72% threshold, probing every 32 admissions. Queue growth resumes on a later probe as soon as heap use is below 72%.

`maximumOutstandingCandidateTasks` bounds queued task descriptors. With 8 workers and a limit of 1024, the intended bound is approximately 1024 queued tasks plus 8 running tasks. The Java priority queue itself is unbounded, so the semaphore invariant must never be bypassed for queued tasks.

Candidate creation rule:

- acquire a queue slot first;
- only then allocate `CandidateTask` and enqueue it;
- if no slot is available, process the candidate synchronously using method parameters, without allocating a task and without incrementing asynchronous `pending` work.

Initial candidates follow the same rule and are no longer materialized as a full `List<CandidateTask>`.

### Default threshold raising and ordering

- Transaction-utility witness raising defaults to enabled.
- Global exact-pair raising defaults to enabled during the initial statistics scan. Each pair is offered exactly once and represented in the same TWU order as the miner so top-k tie-breaking remains deterministic.
- Direct-U child pre-evaluation remains implemented and defaults to disabled.
- Transaction merging remains implemented. Check the source default before each experiment; it is currently enabled in the reset code.
- SU pruning remains implemented and defaults to enabled.
- Threshold-ready DFS warm-up remains implemented and defaults to disabled, so the candidate pool starts immediately.
- Small-subtree DFS remains implemented with `minimumTransactionsPerCandidateTask=512` by default.
- Best-child continuation remains implemented and defaults to disabled.
- Exact utility raises the threshold; it is not the queue priority.
- `workAwarePriority=true` uses `SU / estimatedWork` only as a pool scheduling score. It is not a pruning bound and cannot change correctness.
- `estimatedWork = parentTransactionCount + sum(suffixLengthFromCandidate)` is accumulated in the existing LU/SU scan. Root estimates come from the root-pair scan; deeper estimates come from the projected-node scan. There is no extra database pass or pair matrix for this score.
- Root candidates are seeded in descending work-aware order so workers cannot consume low-priority roots merely because the producer has not enqueued the stronger roots yet.
- `topKThresholdCertified` becomes true when either the exact heap contains `k` patterns or `k` safe singleton/transaction witnesses certify a lower bound; it is informational and no longer gates pool startup.

### Shared top-k correctness

- `minUtil` is `volatile` and never decreases.
- All `PriorityQueue<TopKPattern>` mutations are inside `synchronized (topKQueue)`.
- `offerTopK(...)` rejects `utility < minUtil` before acquiring the heap lock, then rechecks after acquiring it.
- Root pairs are already in top-k before mining; `utilityAlreadyOffered` prevents their candidate tasks from inserting them again.

### Current experiment switches

In `testEFIM_PTMChunk.java`:

```java
boolean candidateParallelism = true;
boolean workAwarePriority = true;
boolean diagnosticStatistics = true;
int maximumOutstandingCandidateTasks = 1024;
int candidateWorkers = 8;
```

Experiment log filenames use compact tokens only for active settings:
`<dataset>_<heap>_k<k>_<s|pWorkers>_wad<0|1>_q<limit|a>_d<0|1>_<timestamp>.log`.
The redundant pool-count token has been removed.

## Concurrency invariants

Do not violate these rules:

1. Increment `PartitionRun.pending` before publishing a task to the shared queue.
2. Every queued task must call `finishOne()` exactly once.
3. A synchronous inline candidate is part of its caller's active work and must not independently increment/decrement `pending`.
4. Release a queue semaphore slot as soon as a worker removes the corresponding task from the queue.
5. Never enqueue a task without first acquiring a queue slot.
6. Snapshot all child SU and estimated-work values before running any child inline because `WorkerBins` is thread-local but reused and reset recursively.
7. A worker must not mutate a parent projected database that can be referenced by sibling tasks. If transaction merging is enabled for an experiment, it must copy on the first merge before later in-place accumulation.
8. Do not block all workers while attempting to put children into a full bounded queue; that can deadlock. Queue overflow must use synchronous inline processing or another nonblocking, correctness-preserving path.
9. Do not replace `volatile minUtil` with a non-visible ordinary field.
10. Do not access or mutate the global top-k heap concurrently outside its lock.

## Performance instrumentation policy

Hot-path statistics are disabled by default in the algorithm but can now be enabled with `configureDiagnosticStatistics(true)`. Do not enable them in production benchmark runs because they use `LongAdder` updates on candidate admission/processing paths.

Removed instrumentation includes:

- candidate count;
- projected transaction-read count;
- merge count;
- candidate RAM-node count;
- read/write byte counters;
- per-worker statistic accumulation;
- per-candidate memory checks (memory is now sampled only at phase boundaries, after loading a partition, and after completing a partition);
- per-partition progress and `[LOAD RAM]` logging.

The remaining statistics are low-frequency configuration/result fields, total elapsed time, final threshold, and DFS/pool partition counts.

The optional diagnostic fields are candidate count, asynchronous task count, small-subtree DFS switches, and queue-overflow inline count. Do not compare a diagnostic run against a production run.

Peak Java heap is reported as `Max memory MB`. Sampling deliberately avoids the candidate hot path. At run completion the sampled peak is copied into the algorithm instance so sequential DFS/pool verifier runs do not overwrite one another through the singleton `MemoryLogger`.

## Validation

Compile all sources:

```powershell
javac -encoding UTF-8 -d out src/*.java
```

Published TKEH example, fully parallel from the start:

```powershell
java -cp out testCorrectnessParallelVsSerial dataset/tkeh_example.txt 10 4 2147483647 true 8
```

Accidents prefix with work-aware priority enabled:

```powershell
java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32
```

The same prefix with FIFO priority:

```powershell
java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 false 32
```

Arguments for `testCorrectnessParallelVsSerial`:

1. input path
2. k
3. worker count
4. maximum transaction count
5. work-aware SU priority
6. maximum outstanding tasks

Validated results as of 2026-09-09:

- Published TKEH Top-10: oracle = PTM DFS = PTM pool; Table 11 check passes.
- Published TKEH Top-10 after work-aware scheduling: oracle = PTM DFS = PTM pool; `workAware=true`, `maxOutstanding=8`, final `minUtil=34`.
- Accidents first 1000 transactions, `k=1000`: oracle = PTM DFS = PTM pool with work-aware priority both enabled and disabled; final `minUtil=67644`. Commands used `workers=4` and `maxOutstanding=32`.
- Published TKEH Top-10 after removing the six flags: oracle = PTM DFS = PTM pool; pair threshold is 22 and final `minUtil=34`.
- Published TKEH Top-10 passes with `maxOutstanding=8`, exercising bounded-queue/inline behavior.
- Accidents first 100 transactions, `k=1000`, after removing the six flags: oracle = PTM DFS = fully parallel pool; pair threshold is 514 and final `minUtil=7669`.
- Chainstore first 100 transactions, `k=1000`: oracle = PTM DFS = fully parallel pool; final `minUtil=11530`.
- Queue-overflow/inline path was exercised with a queue limit of 8 and passed the published example.
- Earlier chainstore first 50 transactions, `k=500`, with Direct-U and Best-SU both disabled also matched the oracle before the scheduler rewrite.

## Benchmark cautions

- Do not infer speedup from different settings. Compare pool FIFO and work-aware runs using identical input, `k`, heap size, worker count, task admission, and transaction limit. Compare each pool run with DFS separately; DFS deliberately ignores the scheduling flag.
- Run multiple repetitions and alternate order. Partition directory deletion/build and Windows/OneDrive filesystem caching can heavily bias the first run.
- `startTimestamp` currently includes preprocessing, partition-directory cleanup, partition construction, mining, and final result construction.
- A small dataset may not amortize worker/queue startup.
- Equal final `minUtil` is necessary but not sufficient for correctness; compare canonical itemset-to-utility maps with the oracle.
- `PriorityBlockingQueue` is thread-safe. Memory growth is caused mainly by retained task/projected-database references and allocation rate, not by missing queue synchronization.
- A queued task retains its parent projected transaction list, `itemsToKeep`, and prefix. Increasing the outstanding limit can therefore increase live memory even though tasks do not copy the whole database.

## Known issues and next experiments

1. Do not run full `chainstore` with the default-enabled square global pair matrix unless the JVM has enough heap for its roughly 40K-item ID range.
2. Compare outstanding limits 64, 128, 256, and 1024 with identical settings. Track elapsed time and external JVM peak RSS if memory counters remain disabled.
3. Compare candidate pool with 1, 2, 4, and 8 workers. `candidateParallelism=false` is DFS, while `candidateParallelism=true, workers=1` is the scheduler baseline.
4. The global top-k heap remains a serialization point for one million results despite the fast rejection path.
5. The custom queue is bounded logically by a semaphore rather than by the queue implementation. Preserve and test the permit accounting.
6. The partition directory name includes the JVM maximum-memory value and a literal `10`; this naming is awkward and should be cleaned only with care because existing experiment scripts may rely on it.
7. The current experiment runner has `diagnosticStatistics=true`; timed results from it include hot-path `LongAdder` overhead and must not be used as production benchmark numbers.
8. Repeated Accidents runs with the same visible configuration took 243,969 ms and 208,423 ms. Candidate count differed only 0.44%, async tasks 0.73%, small-DFS switches about 0.8%, and queue-inline count 0.3%; search-work differences do not explain the 17.1% time difference. Phase timing and GC/CPU/I/O observation are still required.
9. Root seeding still calls `enqueueCandidate(...)` from the main thread. If fixed admission is full, the main thread may execute inline and temporarily become an additional mining thread.
10. The default-enabled global pair matrix deliberately uses `long[maxItem+1][maxItem+1]`, approximately `8(maxItem+1)^2` bytes plus Java row/reference overhead. It is intended only for datasets with a moderate, reasonably dense item-ID range and can OOM for many or sparse item IDs.
11. Computing exact global pairs costs the sum of squared transaction lengths during the initial statistics scan. Benchmark this scan cost against the mining reduction.
12. The work model is a scheduling heuristic, not a safe pruning bound. Measure threshold-over-time, queue wait, and total time over alternating repeated runs before claiming benefit.
13. Root work is measured before root-level unpromising-item filtering, so it can overestimate the later filtered scan. Deeper-level estimates are collected directly from the projected transactions. Transaction merging can also make the estimate conservative because work is counted before merged records are collapsed.

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
- User update added fixed/adaptive heap-pressure task admission, minimum-transaction task granularity, and optional diagnostic `LongAdder` counters.
- Re-read the updated source and validated compilation plus published TKEH Table 11 using `maxOutstanding=8`, immediate pool mode, Best-SU enabled, Direct-U enabled, continuation disabled, and `minTaskTransactions=1`.
- Analyzed two same-configuration Accidents diagnostic runs: 243,969 ms versus 208,423 ms with nearly equal work counters, indicating runtime-system/I/O/contention variance rather than a 17% change in explored work.
- Added optional global exact-pair threshold raising using a two-dimensional matrix populated during the initial statistics scan.
- Released the global pair matrix before partition construction and prevented duplicate root-pair insertion during partition mining.
- Added the global-pair flag to the experiment log name/header, printed statistics, and correctness-test arguments.
- Ordered globally raised pairs by miner TWU order rather than numeric item ID to preserve deterministic top-k boundary ties.
- Removed the Direct-U, transaction-utility raising, global-pair raising, threshold-ready, transaction-merging, and subtree-pruning flags. Their behavior is now fixed respectively to off, on, on, off, off, and on.
- Removed Direct-U exact child buffers/hot-path scans and removed the unused transaction-merging implementation.
- Simplified the PTM runner, log filename/header, algorithm configuration API, and correctness-test arguments after fixing those strategies.
- Shortened experiment log filenames to compact active-setting tokens and removed the redundant `poolCount` variable/parameter/log field. Full-source compilation passes.
- Removed minimum-transaction task granularity and best-child continuation entirely. Every eligible child now returns to the SU-priority queue, with queue-full work still processed inline.
- Removed their configuration methods, diagnostic field, runner/log tokens (`mt`, `bc`), and correctness-test arguments. TKEH Top-10 and Accidents first 100/Top-1000 still match the serial oracle.
- User reset restored all optimization mechanisms. Corrected the configuration model: Direct-U=false, TU raising=true, global-pair raising=true, threshold-ready=false, merging=false, SU pruning=true, best-child=false, and `minimumTransactionsPerCandidateTask=512` are defaults inside `AlgoEFIM_PTMStyleBaseline`, not switches passed by the main runner.
- Restored the compact runner API without deleting mechanisms: `configureCandidateParallelism(enabled, workers, bestSUFirst)` and `runAlgorithm(k, input, output, maximumTransactions)`. Compilation and the TKEH/Accidents oracle comparisons pass with the internal defaults.
- Removed the separate 60% heap-resume ratio. Adaptive admission now uses one 72% heap-pressure threshold and reports `ADAPTIVE_HEAP_72_PERCENT`.
- Restored peak heap reporting with low-frequency phase/partition-boundary samples. The peak is stored per algorithm instance to keep sequential verifier statistics independent. Compilation and published TKEH Top-10 comparison pass.
- Replaced SU-only pool priority with work-aware scheduling score `SU / estimatedWork`; estimated work is collected inside existing root/deep LU-SU scans using one reusable `long[itemCount+1]` array per workspace.
- Removed priority sorting from serial DFS and small-subtree DFS. Root pool candidates are pre-seeded in score order, queued tasks use the same score, and optional best-child continuation follows the same policy when enabled.
- Renamed the active experiment switch/log token from Best-SU (`su`) to work-aware priority (`wad`). Published TKEH Top-10 and Accidents first 1000/Top-1000 match the TKEH oracle with priority enabled; Accidents also matches with priority disabled.

## Required maintenance for future agents

After every material change:

1. Update the relevant description in **Current algorithm state**.
2. Preserve or revise the **Concurrency invariants**.
3. Add the exact validation command and outcome to **Validation**.
4. Add unresolved risks or follow-up measurements to **Known issues and next experiments**.
5. Append a dated bullet to **Change log**.
6. Never claim a performance improvement from a single, differently configured, or cache-biased run.


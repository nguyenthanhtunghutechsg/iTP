# ParallelMassive Working Notes

This file is the persistent handoff and change tracker for this repository. Read it before modifying the mining code. Update the **Current state**, **Validation**, **Known issues**, and **Change log** sections whenever a material implementation, configuration, correctness, memory, or performance change is made.

## Project objective

Develop and evaluate a PTM-style top-k high-utility itemset miner. The current experimental contribution is candidate-level parallel mining with fixed transaction/global-pair threshold raising, optional `SU / estimatedWork` scheduling, bounded resident work, and a true serial DFS baseline.

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
4. Raise the safe lower bound from distinct transaction witnesses. When global-pair raising is enabled, two-item transactions are excluded from this witness store because those itemsets are already present in the exact pair heap; transaction witnesses then start at length three.
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
- sibling order is unchanged by the pool priority, providing a clean DFS baseline;
- exact candidate utility and LU/SU pruning remain active; Direct-U child pre-evaluation has been removed.

Entry points:

- `minePartitionDepthFirst(...)`
- `mineCandidateDepthFirst(...)`

### Parallel mode

`candidateParallelism=true` selects the custom candidate scheduler.

The scheduler no longer uses `ThreadPoolExecutor` and does not call `submit()` or `execute()` for each candidate. It contains:

- a fixed array of long-lived worker threads;
- one shared `PriorityBlockingQueue<CandidateTask>`;
- descending `SU / estimatedWork` ordering when `workAwarePriority=true`;
- FIFO sequence ordering when `workAwarePriority=false`, retained as the ablation baseline;
- a `Semaphore` that logically bounds resident queued tasks;
- worker loops that call `candidateQueue.take()` directly.

Each queued task contains exactly one candidate. `maximumOutstandingCandidateTasks` must be positive and bounds queued task descriptors. The Java priority queue itself is unbounded, so the semaphore invariant must never be bypassed for queued work.

Candidate creation rule:

- acquire a queue slot first;
- only then allocate `CandidateTask` and enqueue it;
- if no slot is available, process the candidate synchronously using method parameters, without allocating a task and without incrementing asynchronous `pending` work.

Initial candidates follow the same rule and are no longer materialized as a full `List<CandidateTask>`.

### Default threshold raising and ordering

- Transaction-utility witness raising defaults to enabled.
- Global exact-pair raising defaults to enabled during the initial statistics scan. Each pair is offered exactly once and represented in the same TWU order as the miner so top-k tie-breaking remains deterministic.
- Direct-U child pre-evaluation has been removed.
- Transaction merging remains implemented. Check the source default before each experiment; it is currently enabled in the reset code.
- SU pruning remains implemented and defaults to enabled.
- The candidate pool starts immediately; threshold-ready warm-up has been removed.
- Small-subtree DFS remains implemented with `minimumTransactionsPerCandidateTask=512` by default.
- Exact utility raises the threshold; it is not the queue priority.
- `workAwarePriority=true` uses `SU / estimatedWork` as the experimental pool scheduling score. It is not a pruning bound and cannot change correctness.
- `estimatedWork = parentTransactionCount + sum(suffixLengthFromCandidate)` is accumulated in the existing LU/SU scan. Root estimates come from the root-pair scan; deeper estimates come from the projected-node scan. There is no extra database pass or pair matrix for this score.
- Root candidates are seeded in descending work-aware order so workers cannot consume low-priority roots merely because the producer has not enqueued the stronger roots yet.

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
int maximumOutstandingCandidateTasks = 128;
int candidateWorkers = 8;
```

Experiment log filenames use compact tokens only for active settings:
`<dataset>_<heap>_k<k>_<s|pWorkers>_wad<0|1>_q<limit>_d<0|1>_<timestamp>.log`.
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

Arguments for `testCorrectnessParallelVsSerial`:

1. input path
2. k
3. worker count
4. maximum transaction count
5. work-aware SU priority
6. maximum outstanding tasks
7. diagnostic statistics (optional; default false)
8. minimum transactions per candidate task (optional; default 512)

Validated results as of 2026-09-10:

- Published TKEH Top-10: oracle = PTM DFS = PTM pool; Table 11 check passes.
- Published TKEH Top-10 after work-aware scheduling: oracle = PTM DFS = PTM pool; `workAware=true`, `maxOutstanding=8`, final `minUtil=34`.
- Accidents first 1000 transactions, `k=1000`: oracle = PTM DFS = PTM pool with work-aware priority both enabled and disabled; final `minUtil=67644`. Commands used `workers=4` and `maxOutstanding=32`.
- Published TKEH Top-10 after removing the six flags: oracle = PTM DFS = PTM pool; pair threshold is 22 and final `minUtil=34`.
- Published TKEH Top-10 passes with `maxOutstanding=8`, exercising bounded-queue/inline behavior.
- Accidents first 100 transactions, `k=1000`, after removing the six flags: oracle = PTM DFS = fully parallel pool; pair threshold is 514 and final `minUtil=7669`.
- Chainstore first 100 transactions, `k=1000`: oracle = PTM DFS = fully parallel pool; final `minUtil=11530`.
- Queue-overflow/inline path was exercised with a queue limit of 8 and passed the published example.
- Earlier chainstore first 50 transactions, `k=500`, with Direct-U and Best-SU both disabled also matched the oracle before the scheduler rewrite.
- After excluding duplicate two-item transaction witnesses, Accidents first 100 transactions passed: `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 100 true 32`; oracle = PTM DFS = PTM pool, final `minUtil=7669`. Observed demo times were 493 ms DFS and 117 ms pool, but this single small cold/warm run is correctness evidence only.
- The documented TKEH command could not be rerun on 2026-09-10 because `dataset/tkeh_example.txt` is absent from the current workspace.
- After removing the failed adaptive-frontier, work-balanced-batch, and load-aware-admission implementations, fixed batching passed Accidents first 1000/Top-1000: `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 8`; oracle = DFS = pool, final `minUtil=67644`. The pool processed 218 asynchronous candidates in 37 fixed batches. This bounded run is correctness/path evidence only.
- Bounded best-child depth 0 and 1 both passed Accidents first 5000/Top-1000: `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 5000 true 32 true 1 512 0` and the same command ending in `1`; oracle = DFS = pool, final `minUtil=352943`. Depth 1 exercised 9411 continuations and reduced candidates from 148130 to 135913, async tasks from 13970 to 10875, and queue-inline candidates from 5921 to 3651. Observed pool times were 1056 and 1020 ms; these bounded runs are correctness/path evidence only.
- After removing alternate scheduler policies, Accidents first 1000/Top-1000 passed with `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512`; oracle = DFS = work-aware pool, final `minUtil=67644`. No full database was run locally. The TKEH example could not be rerun because `dataset/tkeh_example.txt` is absent.
- The cleaned fixed-frontier inline path passed Accidents first 100/Top-1000 with both `workAware=true` and `false`: `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 100 true 4 true 1` and the same command with argument 5 set to `false`; oracle = DFS = pool, final `minUtil=7669`, and both runs exercised queue-overflow inline execution.
- After removing the rejected local top-k buffer, `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512` passed: oracle = DFS = Work-aware SU pool, 1000 results, final `minUtil=67644`.

## Benchmark cautions

- Do not infer speedup from different settings. Compare pool and DFS using identical input, `k`, heap size, worker count, task admission, and transaction limit.
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
14. Transaction-witness distinctness must be preserved across threshold sources. With global exact pairs enabled, never re-admit length-two full transactions as separate witnesses.

## Change log

### 2026-09-10

- Tested and removed a worker-local top-k buffer of 16 after the user-run full Accidents comparison. Against the same-version `wad1/q128/d1` baseline, `lb16` took 182158 versus 180439 ms (0.95% slower), explored 17487807 versus 17457375 candidates, and increased queue-inline work from 627323 to 653649. It grouped 5259463 offers into 329155 flushes, but delayed threshold publication outweighed the reduced heap locking. Final results and `minUtil=14721418` matched. Removed the implementation, API, counters, and `_lb` log token; retained Work-aware SU as the final contribution.
- Simplified the active implementation to candidate-per-task scheduling with a fixed semaphore-bounded queue and the existing 512-transaction DFS fallback. Removed batching, bounded best-child continuation, threshold-ready startup, Direct-U child scans, heap-pressure admission, and their APIs/counters/log fields. Retained `workAwarePriority` as the paper-ablation flag: `true` selects `SU / estimatedWork`, while `false` selects FIFO. The runner emits `_wad<0|1>_q<limit>` filenames. Compiled all sources and validated Accidents first 1000/Top-1000 against the independent oracle; no full database was run locally.
- Re-read the reset implementation and synchronized this document with the active strategy: global exact-pair raising, transaction witnesses, immediate pool startup, work-aware `SU / estimatedWork` priority, 512-transaction whole-subtree DFS fallback, and fixed-1024 runner admission.
- Fixed duplicate threshold certification across sources by excluding two-item transaction certificates when the same itemsets are already seeded through global exact-pair raising.
- Compiled all sources and validated Accidents first 100/Top-1000 against the independent TKEH oracle; no full-database benchmark was run per user request.
- Added the initial threshold-growth Adaptive Frontier experiment. The reported full Accidents run switched at `minUtil=345075` versus final `14721418`, increased candidates by 5.3%, and took 244352 ms versus the 185358 ms `af0` run, so the one-way threshold detector was rejected.
- Replaced it with a reversible Demand-Driven Adaptive Frontier: maintain approximately two shared descendant tasks per worker, continue extra work locally, and donate eligible heavy descendants whenever workers wait. No threshold-growth percentage remains.
- Added universal reserved/queued-task accounting for the demand signal and retained the semaphore as the hard bound. Updated diagnostics to report demand-local continuations and donations.
- Compiled all sources and validated Accidents first 1000 and first 5000 against the independent TKEH oracle. The 5000-prefix run exercised both local continuation and donation; no full database was run.
- Recorded the user-run full Accidents Top-100000 demand-driven ablation: final results/minimum utility remained correct, but `af1` took 233320 ms versus 185358 ms for `af0`, explored 18033352 versus 15989812 candidates, created 166627 versus 1639618 async tasks, and sampled 1794 MB versus 1290 MB peak heap. The demand-driven policy is therefore retained only as a failed experimental switch, not the recommended mode.
- Added priority-aware sibling batching with configurable `candidateBatchSize`; batch 1 preserves candidate-per-task behavior and the runner now selects batch 8 with `adaptiveFrontier=false`.
- Added batch size to run headers/log filenames and added diagnostic asynchronous-candidate plus realized-average-batch reporting.
- Compiled all sources and validated batch sizes 1 and 8 on Accidents first 1000/Top-1000 against the independent TKEH oracle. No full database was run.
- Recorded the user-run full Accidents `b8/q1024` result: 3.36% fewer candidates and 64.3% fewer async tasks with essentially unchanged elapsed time, but 33.8% greater sampled heap. Changed the runner's next batching experiment to `q=128` so its maximum queued-candidate capacity is approximately comparable to `b1/q1024`.
- Tested bypassing sibling ordering before the small-DFS path on a bounded prefix; candidates increased from roughly 100.4K to 119.4K, showing that work-aware sibling order contributes useful threshold raising. Reverted that micro-optimization and retained ordered small-subtree DFS.
- Recorded the user-run full Accidents `b8/q128` result: the result set/final threshold matched, candidates fell 3.53%, asynchronous tasks fell 66.38%, and diagnostic elapsed time fell 4.51% relative to `b1/q1024`. Because diagnostics remained enabled and peak-heap sampling increased, this is promising evidence rather than a final speedup claim.
- Added optional adaptive work-balanced batching. It derives contiguous batch boundaries from remaining estimated work while retaining work-aware order, maximum batch size, and batch count; the runner uses `b8/wb1/q128`.
- Added the `wb` log token/configuration field and validated `b8/wb1` on Accidents first 1000/Top-1000 against the independent oracle. No full database was run.
- Added diagnostics for average and maximum estimated work per asynchronous batch so fixed-width and work-balanced runs can be compared for load dispersion; these counters remain disabled in production timing runs.
- Recorded the user-run full Accidents `b8/wb1/q128` result. Work balancing reduced sampled heap but slightly increased time, candidates, and tasks relative to fixed `b8/q128`; the roughly 70x maximum-to-average batch-work ratio indicates a single-candidate heavy tail. Restored fixed batching as the runner default while retaining `wb1` as an ablation switch.
- Added optional estimated-load-aware admission (`la`): lightweight atomic queued/active work accounting, per-worker remaining batch work, and inline continuation when all other workers already hold at least as much average work as the proposed branch.
- Added `la` configuration/logging and a diagnostic load-aware-inline count. Compiled all sources and validated both a 1000-transaction prefix and a 5000-transaction path-coverage run against the TKEH oracle; no full database was run.
- Recorded the user-run full Accidents `b8/wb0/la1/q128` result: the original active-plus-queued formula over-inlined 1603767 candidates and regressed elapsed time from 177005 to 197240 ms, so that formula was rejected.
- Corrected load-aware admission to compare a proposed descendant only with work held by the other active workers, excluding queued backlog. A candidate's estimated weight remains active until its branch finishes so a recursively busy worker is not mistaken for an idle worker. Recompiled and validated Accidents first 5000/Top-1000 against the independent oracle; the bounded run retained 1286 asynchronous batches and exercised 11622 load-aware inline candidates. No full database was run.
- Recorded the user-run corrected active-only `b8/wb0/la1/q128` result: it improved over the rejected queued-load formula but remained 7.81% slower than `la0` and explored 2.29% more candidates. Restored `loadAwareAdmission=false` as the runner and correctness-test default; `la1` remains available as a paper ablation.
- Removed the failed adaptive-frontier, work-balanced-batching, and load-aware-admission implementations, configuration methods, counters, log tokens, and correctness-runner arguments. The active pool strategy is now work-aware priority with fixed-width sibling batching and bounded queue admission. Recompiled all sources and validated Accidents first 1000/Top-1000 with `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 8`; oracle = DFS = pool and final `minUtil=67644`. No full database was run.
- Replaced the priority denominator `parentTransactionCount + suffixWork` with candidate-specific `projectedSupport + suffixWork`, implementing support-aware utility-density selection: high-SU branches occurring in fewer projected transactions receive higher priority. Added one lazily reset support array to each root/worker workspace and incremented it inside the existing LU/SU scans, so there is no extra database pass. Renamed user-facing priority labels and the log token from `wad` to `sad`. Recompiled and validated Accidents first 1000/Top-1000 against the independent oracle with the same bounded command; final `minUtil=67644`, no full database was run.
- Rejected and removed support-aware utility-density selection after the user-run equal-configuration `b1/q128/d1` comparison. Versus work-aware WAD, support-aware SAD took 220537 versus 183323 ms (20.30% slower), explored 18724492 versus 17368557 candidates (7.80% more), submitted 1485088 versus 1172356 tasks (26.67% more), and sampled 1406 versus 1292 MB heap. Rare projected support did not predict a tight SU or early threshold gain. Restored `SU / (parentTransactionCount + suffixWork)`, the `wad` log token, and all WAD labels; recompiled and validated Accidents first 1000/Top-1000 against the independent oracle, final `minUtil=67644`. No full database was run.
- Tested threshold-margin-density ordering, `(SU - minUtilSnapshot) / estimatedWork`, only on the bounded Accidents first 1000/Top-1000 run. It passed the oracle but explored about 108311 candidates versus roughly 100453 for restored WAD under the same small configuration, so it was rejected without spending a full-database run and removed from the code.
- Added optional threshold-neutral top-k tie buffering (`tb`): pool workers buffer only exact patterns equal to `minUtil` after the exact heap is full, flush 64 ties under one lock, publish every greater utility immediately, and flush in `finally` before worker shutdown. Added diagnostic tie/flush counts and the `_tb` log token. Compiled all sources and validated the forced-path command `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 8 true 1`; oracle = DFS = pool, final `minUtil=67644`, with 22 buffered ties and 4 flushes. No full database was run.
- Removed threshold-neutral buffering after the user-run full Accidents `wad1/b1/tb1/q128/d1` result showed only 200 buffered ties and 8 flushes. Elapsed time was 179306 versus 183323 ms for the prior no-buffer run, but avoiding roughly 192 heap locks cannot credibly explain 4017 ms; candidates also rose 0.13%, showing ordinary parallel/JVM variation. The mechanism, counters, API, log token, and runner arguments were removed to keep the hot path clean. Recompiled and validated the restored WAD pool on Accidents first 1000/Top-1000; oracle = DFS = pool and final `minUtil=67644`. No full database was run locally.
- Reworked best-child continuation into bounded best-path lookahead. Each task receives a configurable depth budget; the runner uses exactly one local best-WAD child before flushing the remaining frontier through normal queue admission. Removed the old unbounded boolean activation path, added `_lc<depth>` logging and a diagnostic continuation count. Both `lc0` and `lc1` passed the independent oracle on Accidents first 5000/Top-1000; `lc1` reduced candidates 8.25%, async tasks 22.15%, and queue-inline candidates 38.34% on that bounded run. No full database was run locally.
- Recorded the user-run full Accidents `wad1/b1/lc1/q128/d1` result: final correctness indicators matched, elapsed time improved 2.53%, async tasks fell 30.68%, and queue-inline candidates fell 26.46% versus `lc0`, while sampled heap rose 8.80%. Retained depth 1 as the current runner setting but treated the single diagnostic result as near-plateau evidence rather than a proven speedup.

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

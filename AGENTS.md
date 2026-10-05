# ParallelMassive Working Notes

This file is the persistent handoff and change tracker for this repository. Read it before modifying the mining code. Update the **Current state**, **Validation**, **Known issues**, and **Change log** sections whenever a material implementation, configuration, correctness, memory, or performance change is made.

## Project objective

Develop and evaluate a PTM-style top-k high-utility itemset miner. The current experimental contribution is partition- and candidate-level parallel mining with fixed transaction/global-pair threshold raising, optional `SU / estimatedWork` scheduling, bounded resident work, optional object-free projected databases, and a true serial DFS baseline.

Correctness has priority over speed. Any parallel change must be compared against the independent serial TKEH verifier before it is treated as valid.

## Important files

- `src/AlgoEFIM_PTMStyleBaseline.java`: main PTM-style serial/parallel implementation.
- `src/AlgoTKEHSerialVerifier.java`: independent serial TKEH correctness oracle.
- `src/testCorrectnessParallelVsSerial.java`: compares TKEH oracle, PTM serial DFS, and PTM parallel result sets.
- `src/testEFIM_PTMChunk.java`: main experiment runner; redirects output to a timestamped log file.
- `src/testTKEHSerial.java`: serial verifier runner and result logger.
- `src/Transaction.java`: transaction and projected-transaction representation.
- `tools/plot_minutil_convergence.py`: dependency-free SVG comparison tool for threshold trace CSV files.

## Current algorithm state

### Shared preprocessing

1. Discover the maximum item identifier, then scan the input for TWU, support, singleton utility, transaction-utility witnesses, and exact global pair utility.
2. Apply RIU using exact singleton utilities.
3. Offer every observed exact 2-itemset from the global `long[maxItem+1][maxItem+1]` matrix to top-k, then release the matrix before partition construction.
4. Raise the safe lower bound from distinct transaction witnesses. When global-pair raising is enabled, two-item transactions are excluded from this witness store because those itemsets are already present in the exact pair heap; transaction witnesses then start at length three.
5. Rename promising items.
6. Build PTM prefix partitions by one reduced-database pass.
7. With `workAwarePriority=true`, order mineable partitions by descending `partitionSU / estimatedPartitionWork`; otherwise retain average suffix transaction utility as the ablation baseline.
8. Load one partition fully into RAM.
9. Compute root-pair LU, SU, and estimated scan work with reusable one-dimensional workspaces. Exact pair utility is not recomputed or offered during partition mining.

### Serial mode

`candidateParallelism=false` selects genuine recursive DFS:

- no candidate scheduler;
- no worker threads;
- no shared candidate frontier;
- sibling order is unchanged by the pool priority, providing a clean DFS baseline;
- partition order follows the shared Work-aware SU flag in both DFS and pool modes;
- exact candidate utility and LU/SU pruning remain active; Direct-U child
  pre-evaluation has been removed.

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

After a queued candidate finishes, its task explicitly nulls the parent
projected database, `itemsToKeep`, prefix, and `PartitionRun` references before
the worker blocks on the queue again. This prevents a worker's last stack-local
task from pinning a completed projection/lazy-ancestor chain while idle; it
does not force garbage collection.

Projected databases no longer require one `Transaction` wrapper per projected
occurrence. `objectFreeProjection=true` stores the packed base-transaction
index/offset, prefix utility, and remaining utility in segmented primitive
blocks. The object-free root is a zero-copy metadata view: it reads offset,
prefix utility, and remaining utility directly from the already loaded
partition list and allocates neither root descriptor blocks nor a copied
transaction-reference array. Only projected children materialize descriptors.
Base item/utility arrays are immutable and shared for the life of one partition.
`false` retains an object-wrapper ablation with identical search semantics.
Transaction merging is removed from both modes so the comparison isolates
representation/allocation cost.

Optional `memoryBoundedProjection` caps newly materialized object-free
descriptor blocks within each partition. A candidate reserves its worst-case
block requirement before construction. If the reservation cannot be made, the
child is represented as a lazy `(parent, extension)` view; later scans replay
the parent cursor and reconstruct projection metadata without descriptor
allocation or disk spill. Unused worst-case reservations are returned after
construction. After loading each root partition, the configured cap is reduced
to the currently available heap minus a safety reserve of the greater of 64 MB
or 20% of `Xmx`; statistics report the minimum/maximum effective cap. The
budget is cumulative within a partition (allocated blocks
are not recycled into the counter) to avoid ownership/ref-counting on the
parallel frontier. This bounds only child projection descriptors, not the
loaded root partition, top-k heap, workspaces, task objects, trace queue, or
JVM overhead.

### Default threshold raising and ordering

- Transaction-utility witness raising defaults to enabled.
- Global exact-pair raising defaults to enabled during the initial statistics scan. Each pair is offered exactly once and represented in the same TWU order as the miner so top-k tie-breaking remains deterministic.
- Direct-U child pre-evaluation has been removed.
- Transaction merging has been removed from the mining path.
- SU pruning remains implemented and defaults to enabled.
- The candidate pool starts immediately; threshold-ready warm-up has been removed.
- Small-subtree DFS remains implemented with `minimumTransactionsPerCandidateTask=512` by default.
- Exact utility raises the threshold; it is not the queue priority.
- `workAwarePriority=true` uses `SU / estimatedWork` as the experimental pool scheduling score. It is not a pruning bound and cannot change correctness.
- `estimatedWork = parentTransactionCount + sum(suffixLengthFromCandidate)` is accumulated in the existing LU/SU scan. Root estimates come from the root-pair scan; deeper estimates come from the projected-node scan. There is no extra database pass or pair matrix for this score.
- `workAwarePriority=true` also uses `partitionSU / (transactionCount + sumSuffixLength)` for partition priority. `partitionSU` includes only records with at least one extension, preventing singleton-only occurrences from inflating a partition's score. All three values are collected while writing partitions, so partition WAD adds no input scan. `false` restores both average-transaction-utility partition order and FIFO candidate scheduling.
- Root candidates are seeded in descending work-aware order so workers cannot consume low-priority roots merely because the producer has not enqueued the stronger roots yet.

### Shared top-k correctness

- `minUtil` is `volatile` and never decreases.
- All `PriorityQueue<TopKPattern>` mutations are inside `synchronized (topKQueue)`.
- `offerTopK(...)` rejects `utility < minUtil` before acquiring the heap lock, then rechecks after acquiring it.
- Root pairs are already in top-k before mining; `utilityAlreadyOffered` prevents their candidate tasks from inserting them again.
- Threshold events are enqueued only after a strict `minUtil` increase while the top-k lock is held, preserving their order. A dedicated writer thread performs CSV I/O outside the heap lock.

### Current experiment switches

In `testEFIM_PTMChunk.java`:

```java
ExperimentMode defaultExperimentMode = ExperimentMode.SERIAL_BASELINE;
boolean diagnosticStatistics = true;
boolean objectFreeProjection = true;
boolean automaticProjectionBudget = true;
int explicitProjectionBudgetMb = 256;
int maximumOutstandingCandidateTasks = 16;
int defaultCandidateWorkers = 8;
```

The main experiment runner is configured through the single `IDE EXPERIMENT SETUP` block at the top of `testEFIM_PTMChunk.java`. The three cumulative presets are `1=SERIAL_BASELINE` (candidate pool, MBP, and WAD off), `2=PARALLEL_MBP` (configured workers and MBP on, FIFO/average-TU ordering), and `3=PARALLEL_MBP_WAD` (parallel, MBP, and Work-aware SU on). Object-free projection remains a shared representation switch outside these three presets. Set the input, Top-k array, transaction limit, default mode/worker count, queue limit, object-free projection, automatic/explicit MBP budget, and diagnostics in the same block. The optional CLI syntax is `testEFIM_PTMChunk [mode:1|2|3] [candidateWorkers]`; no arguments use the IDE defaults, mode 1 always runs one effective worker, and modes 2/3 use the optional worker count or its IDE default. Automatic budget selects 25% of maximum heap with a 64 MB floor and 512 MB cap; setting `automaticProjectionBudget=false` uses `explicitProjectionBudgetMb`. Every run creates a fresh algorithm instance, rebuilds partitions, resets mining statistics/threshold/memory sampling, joins its workers and trace writer, closes its log, and restores console streams and the previous uncaught-exception handler. Run timestamps include milliseconds; existing output directories receive a collision suffix rather than being overwritten. The batch stops on a run failure.

The algorithm now deletes its owned temporary partition directory in a run-level `finally`, on normal completion or Java exceptions, after the mining scheduler's shutdown path. Cleanup does not follow symbolic links and never targets input files or `result/`. Cleanup failures are surfaced (or suppressed onto an existing run failure) rather than ignored. Partition cleanup occurs after the reported algorithm end timestamp, so reported `Time ms` excludes final directory deletion.

Experiment log filenames use compact tokens only for active settings:
`<dataset>_<heap>_k<k>_<s|pWorkers>_wad<0|1>_ofp<0|1>[_mbp[a]<MB>]_q<limit>_d<0|1>_<timestamp>.log`, where `a` marks the automatic budget policy.
The redundant pool-count token has been removed.

Each experiment creates `result/<run-name>/`, where the run directory uses the existing log stem. It contains `<run-name>.log` and `<run-name>_threshold.csv`. The CSV records every strict `minUtil` increase plus `RUN_START`, `PARTITION_START`, and `RUN_END`, with columns `sequence,event,source,elapsed_ms,wall_time_ms,partition_index,partition_item,min_util`. Partition indices are zero-based among actually started partitions, and partition items use original identifiers.

`tools/plot_minutil_convergence.py` compares one or more threshold traces without loading millions of rows into memory. Its default output is a standalone LaTeX PGFPlots `.tex` figure plus one downsampled sidecar CSV per trace; the LaTeX reads those files through `\addplot table` instead of embedding coordinates. `.svg` remains available by choosing an SVG output filename. Both plot absolute `minUtil` against elapsed time, carry each final `minUtil` to the shared maximum time, and use an open-circle marker to distinguish actual completion from the carried segment.

## Concurrency invariants

Do not violate these rules:

1. Increment `PartitionRun.pending` before publishing a task to the shared queue.
2. Every queued task must call `finishOne()` exactly once.
3. A synchronous inline candidate is part of its caller's active work and must not independently increment/decrement `pending`.
4. Release a queue semaphore slot as soon as a worker removes the corresponding task from the queue.
5. Never enqueue a task without first acquiring a queue slot.
6. Snapshot all child SU and estimated-work values before running any child inline because `WorkerBins` is thread-local but reused and reset recursively.
7. A worker must not mutate a parent projected database that can be referenced by sibling tasks.
8. Do not block all workers while attempting to put children into a full bounded queue; that can deadlock. Queue overflow must use synchronous inline processing or another nonblocking, correctness-preserving path.
9. Do not replace `volatile minUtil` with a non-visible ordinary field.
10. Do not access or mutate the global top-k heap concurrently outside its lock.
11. Object-free child databases may append only to their own primitive array;
    base transactions and every parent descriptor array are immutable to them.
12. `transactionId`, `offset`, `prefixUtility`, and `remainingUtility` must be
    carried together for every projected occurrence. Offset alone is not
    sufficient to recover the utility of a non-contiguous itemset prefix.
13. The object-free root view must remain immutable and descriptor-free. Its
    logical transaction id is the index into `baseTransactions`; only child
    projected databases may allocate and append descriptors.
14. Lazy projections are immutable replay views. Every scan must open a new
    cursor; never share cursor state between workers or clear/mutate a parent
    while descendants may still reference it.
15. Reserve projection blocks atomically before materialization. A failed
    reservation must create a fully lazy child; never construct a partially
    materialized child or block workers waiting for descriptor budget.
16. A completed `CandidateTask` must clear its heavy references before calling
    `finishOne()` and returning to `queue.take()`. Preserve the local
    `PartitionRun` reference until `finishOne()` has executed exactly once.

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

The optional diagnostic fields are candidate count, asynchronous task count,
small-subtree DFS switches, and queue-overflow inline count. Do not compare a
diagnostic run against a production run.

Threshold tracing is independent experiment instrumentation. It timestamps every threshold change and uses a lossless asynchronous event queue with periodic writer flushes. Compare traced runs only with other traced runs because event allocation, writer CPU, and disk I/O add overhead.

Peak Java heap is reported as `Max memory MB`. Sampling deliberately avoids the candidate hot path. At run completion the sampled peak is copied into the algorithm instance so sequential DFS/pool verifier runs do not overwrite one another through the singleton `MemoryLogger`.

## Validation

- 2026-10-04: Consolidated the main experiment configuration into one IDE
  setup block and removed command-line parsing from `testEFIM_PTMChunk`.
  Added three type-safe cumulative presets: serial baseline, parallel+MBP,
  and parallel+MBP+WAD; compilation and `git diff --check` passed after the
  preset wiring change. Added validated numeric CLI mode selection (`1..3`)
  plus an optional worker-count override while keeping dataset and remaining
  experiment settings in the IDE block.
  Recompiled all sources and reran the independent verifier on Accidents first
  100 transactions/Top-1000; oracle, DFS, and pool matched with final
  `minUtil=7669`. The algorithm and verifier interfaces were unchanged.

- 2026-10-03: Tested and removed indexed-lazy parent-ordinal projection. On a
  SUSY prefix-100000/Top-2000 diagnostic pair it reduced candidates from
  20,889 to 17,808 but increased time from 4,455 ms to 5,937 ms while sampled
  heap stayed near 306 MB. After complete removal, recompilation and
  `java -Xms512m -Xmx1g -cp out/production/iTP testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512 - true true 1`
  passed: oracle, DFS, and pool returned 1000 patterns with final
  `minUtil=67644`; the pool exercised 23,453 pure-lazy projections.

- 2026-10-02: Added a per-partition heap-headroom guard beneath the configured
  MBP cap and retained a 20%/64 MB reserve for non-projection allocations.
  Recompiled and reran the forced 1 MB Accidents correctness command; oracle,
  DFS, and pool matched with 1000 patterns and final `minUtil=67644`, while the
  pool exercised 23,421 lazy projections and reported an effective range of
  1.0--1.0 MB. Repeating the user's full SUSY Top-2000, 8-worker, `q=16`,
  600 MB heap, auto-150 MB run passed the former descriptor-OOM position but
  still failed while loading the next root partition at 77/105. The failure is
  now outside child projection and shows that MBP alone cannot guarantee a
  partition fits in a 600 MB heap.

- 2026-10-02: Completed asynchronous tasks now sever references to their
  parent projection, keep-array, prefix, and run before an idle worker blocks
  for its next task. Compiled all sources and reran the forced 1 MB lazy path:
  `java -Xms512m -Xmx1g -cp out/production/iTP testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512 - true true 1`.
  Oracle, DFS, and pool matched with 1000 patterns and final `minUtil=67644`;
  the pool exercised 23,474 lazy projections. This validates correctness and
  path cleanup only; rerun the full 10 GB experiment before claiming a memory
  or runtime improvement.

- 2026-10-02: Changed the experiment runner's default MBP budget to automatic
  `max(64 MB, min(512 MB, Xmx/4))`, while retaining a numeric CLI override and
  accepting the literal `auto`. Recompiled all sources. Smoke runs on ten
  Accidents transactions confirmed `-Xmx1g -> mbpa256` and
  `-Xmx10000m -> mbpa512`; both completed successfully with final
  `minUtil=1185`. Automatic runs carry `_mbpa<actualMB>` in the filename and
  identify the policy in the header.

- 2026-10-02: Full 5,000,000-row SUSY stress run completed under a 1 GB heap
  with memory-bounded replay enabled: `java -Xms1g -Xmx1g -cp out/production/iTP testEFIM_PTMChunk 1000 dataset/SUSY_Utility.txt 2147483647 true true 64`.
  Configuration was 8 workers, `q=16`, WAD/object-free enabled, diagnostics
  and threshold tracing enabled. It processed all 99 partition positions,
  returned 1000 patterns, reported final `minUtil=117279927`, 10,131 lazy
  projections, 176,950 ms, and 869.11 MB sampled peak heap; temporary
  partitions were deleted and status was SUCCESS. This establishes OOM-path
  survival for this configuration, not result correctness on SUSY (item 0 and
  duplicate identifiers remain noncanonical) or comparative speedup.

- 2026-10-02: Added optional memory-bounded lazy projection replay. Compiled
  all sources to `out/production/iTP`. Forced the lazy path with only a 1 MB
  per-partition descriptor budget using
  `java -Xms1g -Xmx1g -cp out/production/iTP testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512 - true true 1`.
  It created 23,474 lazy projections in the diagnostic pool run; the
  independent TKEH oracle, PTM DFS, and PTM pool returned the same 1000
  patterns with final `minUtil=67644`. Repeated with the feature disabled via
  the same command ending in `true false 256`; oracle, DFS, and pool again
  matched at `minUtil=67644`. These are bounded correctness/path tests, not
  performance or full-SUSY OOM evidence. Two separate-JVM SUSY-prefix smoke
  runs (`100000` transactions, Top-1000, 8 workers, `q=16`, diagnostics on,
  1 GB heap) both completed: `mbp64` reported 3992 ms, 530.69 MB sampled heap,
  5816 lazy projections, and final `minUtil=2338424`; disabled MBP reported
  3208 ms, 613.64 MB, zero lazy projections, and the same final threshold.
  This single ordered pair suggests the expected memory/time trade-off only;
  cache/JIT/order and SUSY's invalid set semantics prevent a performance or
  correctness claim.

- 2026-10-02: Removed the rejected conditional pair-suffix experiment and
  restored the LU/SU-only candidate path. Compiled all sources to
  `out/production/iTP`; Accidents first 1000/Top-1000 with `workers=4`, `q=32`,
  WAD/object-free enabled and diagnostics enabled matched the independent TKEH
  oracle in DFS and pool modes with 1000 patterns and final `minUtil=67644`.

- 2026-10-01: After removing the rejected parent-work-group experiment,
  compiled all sources to `out/production/iTP` and ran Accidents first
  1000/Top-1000 with `workers=4`, `q=32`, WAD/object-free enabled, diagnostics
  enabled, and the normal 512-transaction cutoff. The independent TKEH oracle,
  PTM DFS, and restored candidate-per-task pool returned the same 1000 patterns
  with final `minUtil=67644`.

- 2026-10-01: User-run full SUSY/Top-1000 with the zero-copy root, FIFO
  candidate pool, 8 workers, `q=1024`, `ofp1/d1`, and a 1504 MB heap exhausted
  the heap while a worker allocated a child descriptor block. It reached
  partition 63/99 after about 121 seconds total (roughly 71 seconds before
  mining and 50 seconds mining) and did not complete. The trace file was about
  776 KB. Compared with the serial 1 GB run reaching partition 89/99 before
  OOM, this demonstrates strong early throughput but substantially higher live
  projection/retained-parent memory; progress-to-failure is not a valid speedup
  comparison. Test smaller queue limits before changing the scheduler.

- 2026-10-01: User-run full SUSY/Top-1000 after zero-copy root, serial DFS,
  `wad0/ofp1/d1`, and `-Xms1g -Xmx1g` reached partition 89 of 99 before
  exhausting the heap after about 641 seconds. The failure remained at
  `newDescriptorBlock`, reached from three nested `mineCandidateDepthFirst`
  calls, so the root descriptor layer was removed successfully but the live
  child/ancestor descriptor chain still exceeded 1 GB. The completed trace was
  only about 2 MB on disk, so this run does not indicate trace storage as the
  dominant allocation. The run did not complete and provides no speedup claim.

- 2026-10-01: Added an object-free root view that allocates no descriptor per
  partition transaction. Compiled directly to IntelliJ's active classpath with
  `javac -encoding UTF-8 -d out/production/iTP src/*.java`. The command
  `java -Xms1g -Xmx1g -cp out/production/iTP testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 false 512 - true`
  matched the independent TKEH oracle in both recursive DFS and pool modes with
  1000 results and final `minUtil=67644`. A forced pool-path first-100 run also
  passed for both `objectFreeProjection=true` and `false` at `minUtil=7669`.
  These runs establish correctness only; full SUSY memory impact is unmeasured.

- 2026-10-01 full SUSY/Top-1000 with serial DFS, object-free projection, and
  `-Xms1g -Xmx1g` exhausted the heap while allocating an additional fixed
  descriptor block (`newDescriptorBlock`). This occurred after replacing the
  contiguous payload with segmented blocks, proving that live descriptor
  volume—not a copy spike—exceeded the heap. The user also observed a slowdown.
  Object-free/no-merge projection is rejected for the target workload.

- 2026-09-30: Replaced per-occurrence projected `Transaction` allocation with
  optional object-free primitive descriptors and removed transaction merging
  from both ablation modes. Compilation and `git diff --check` passed.
  `objectFreeProjection=true` and `false` both matched the independent TKEH
  oracle and recursive DFS on Accidents first 100/Top-1000 with the forced
  `minimumTransactionsPerCandidateTask=1` path (`minUtil=7669`), and on first
  1000/Top-1000 with the normal 512 cutoff (`minUtil=67644`). These bounded
  runs establish correctness only; sampled heap is too coarse for a memory
  claim.

- 2026-09-30: Added estimated-work-triggered intra-candidate parallel
  projection behind an on/off flag. `javac -encoding UTF-8 -d out src/*.java`
  and `git diff --check` passed. The forced-path command
  `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 100 false 32 true 1 - false true 10 8192`
  matched the independent oracle and recursive DFS with 1000 results and
  `minUtil=7669`; it exercised 66,543 projection jobs, 295,251 dynamically
  sized chunks, and 14,883 helper chunks. A 5000-transaction run with a
  100,000-work trigger also
  passed at `minUtil=352943` but intentionally recorded zero eligible jobs,
  confirming this mechanism targets much larger candidates. Earlier
  transaction-trigger prototypes also passed
  Accidents first 5000/Top-1000 at `minUtil=352943`, but those timings are not
  evidence for the final work-triggered implementation. No full massive run
  has been performed locally.

- 2026-09-30: Added ECTP behind an on/off flag. All sources compiled and
  `git diff --check` passed. Forced-pool Accidents first 1000/Top-1000 with
  `minimumTransactionsPerCandidateTask=1` matched the independent oracle and
  DFS at `minUtil=67644`; ECTP combined 31,929 exact offers into 25,086 heap
  batches (maximum batch 5). With the normal 512 cutoff, Accidents first
  5000/Top-1000 and Top-10000 also matched the oracle and DFS at final
  thresholds 352,943 and 292,441. On the single Top-10000 bounded pair, ECTP
  took 3,230 ms versus 3,536 ms, but combined 719,895 offers into 707,475
  batches (average 1.018, maximum 4), so the 8.7% timing difference cannot yet
  be attributed to lock combining. Repeated full runs with diagnostics and
  tracing disabled are required.

- 2026-09-30: Removed fused-sibling projection and MCB from the implementation,
  runner, and verifier. `rg` found no remaining fused/MCB symbols under `src/`,
  all sources compiled, and
  `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 false 32 true 512`
  passed: oracle = recursive DFS = FIFO pool, 1000 results, final
  `minUtil=67644`. `git diff --check` passed.

- 2026-09-30 full Accidents/Top-10000 user run rejected MCB as a performance
  improvement. Against the matching earlier `p8/wad0/fsp0/q1024/d1` baseline,
  `mcb1` took 106,745 ms versus 105,684 ms (+1.0%) and sampled 3939.5 MB versus
  3346.0 MB peak heap (+17.7%). MCB performed 112,790 evaluations, directly
  pruned 97,732 candidates, and reduced total processed candidates from
  1,475,726 to 1,362,687 (-7.7%), but its summed worker cost was 151,169 ms.
  Both runs returned 10,000 results at `minUtil=19091678`. The implementation
  was subsequently removed from the source at the user's request.

- 2026-09-30: Added MCB and compiled all sources. Forced its parallel path with
  `minimumTransactionsPerCandidateTask=1`; Accidents first 100 and first 1000,
  Top-1000 matched both the independent TKEH oracle and recursive PTM DFS with
  final `minUtil` values 7669 and 67644. With the normal 512-transaction DFS
  cutoff, Accidents first 5000/Top-1000 also passed at the final
  `minUtil=352943`. In one diagnostic MCB-on/off pair at 5000 transactions,
  MCB pruned 6,745 candidates at 4,685 evaluated nodes and reduced processed
  candidates from 168,041 to 164,061 (-2.4%), but took 1,360 ms versus
  1,304 ms (+4.3%); summed MCB worker time was 408 ms. This bounded run is
  correctness/path evidence and an early negative performance signal, not a
  full-database conclusion.

- 2026-09-30 full Accidents/Top-10000 user run rejected fused sibling
  projection as a performance improvement. With otherwise matching
  `p8/wad0/q1024/d1` settings, `fsp1/fb8` took 156,081 ms versus 105,684 ms
  for `fsp0` (+47.7%) and sampled 3796.5 MB versus 3346.0 MB peak heap
  (+13.5%). It reduced candidates from 1,475,726 to 1,325,738 (-10.2%),
  asynchronous tasks from 167,630 to 113,852 (-32.1%), and queue-inline
  candidates from 329,151 to 181,844 (-44.8%), but the per-candidate fused
  construction/allocation cost outweighed those reductions. Both modes ended
  at `minUtil=19091678` with 10,000 results. This was one fused-then-unfused
  pair, so exact timing remains order-sensitive, but the regression is too
  large to claim benefit.

- 2026-09-30: Added fixed-batch fused sibling projection and compiled all
  sources. Forced the fused path with `minimumTransactionsPerCandidateTask=1`
  and compared exact itemset-to-utility maps against the independent TKEH
  oracle and recursive PTM DFS. Accidents first 100/Top-1000 and first
  1000/Top-1000 both passed; final `minUtil` values were 7669 and 67644.
  A repeated 1000-transaction diagnostic run exercised 25,322 fused batches
  and 71,861 fused candidates. The unfused FIFO pool was also rerun on the first
  100 transactions and passed. These bounded runs establish correctness/path
  coverage only, not a speedup claim.

- Partition cleanup: compiled all sources, validated Accidents first 10/Top-1000 with WAD enabled against the independent oracle (final `minUtil=1010`), and ran the five-value main on ten transactions. All five runs finished and the exact temporary partition directory was absent afterward; result log/trace files remained.

- 2026-09-13: Compiled all sources and ran `java -cp out testEFIM_PTMChunk 1000,2000,3000,4000,5000 dataset/accidents.txt 10`. All five small runs completed with separate log/trace files and fresh threshold/sequence state. `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 10 false 4` passed the independent oracle with final `minUtil=1010`. No full database was run.

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
9. threshold trace output path (optional; use `-` to disable it while passing the next argument)
10. object-free projection (optional; default true in the verifier)
11. memory-bounded projection (optional; default false in the verifier)
12. projection budget in MB (optional; default 256)

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
- With the single combined WAD flag, `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 1000 true 32 true 512` and the same command with argument 5 set to `false` passed the oracle with 1000 results and final `minUtil=67644`. On bounded diagnostic runs, `wad1` mined 80 partitions and roughly 27.5K candidates versus 144 partitions and roughly 120.5K candidates for `wad0`. This is correctness/path evidence only, not a full-database speedup claim.
- Threshold tracing passed the independent oracle with `java -cp out testCorrectnessParallelVsSerial dataset/accidents.txt 1000 4 100 true 4 true 1 /private/tmp/itp-trace-test/verifier-threshold.csv`; the CSV contained one run start, 75 partition starts, 6792 strict threshold updates, one run end, and ended at `minUtil=7669`, matching the result heap.
- The convergence tool parsed the full Accidents WAD-off trace (3,181,031 rows) and WAD-on trace (770,744 rows), validated monotonic sequence/minimum utility, and rendered a compact LaTeX PGFPlots file that reads two 800-row sidecar CSV files. The common-time curves use final-value carry-forward.

## Benchmark cautions

- Do not infer speedup from different settings. Compare pool and DFS using identical input, `k`, heap size, worker count, task admission, and transaction limit.
- Run multiple repetitions and alternate order. Partition directory deletion/build and Windows/OneDrive filesystem caching can heavily bias the first run.
- `startTimestamp` currently includes preprocessing, partition-directory cleanup, partition construction, mining, and final result construction.
- A small dataset may not amortize worker/queue startup.
- Equal final `minUtil` is necessary but not sufficient for correctness; compare canonical itemset-to-utility maps with the oracle.
- `PriorityBlockingQueue` is thread-safe. Memory growth is caused mainly by retained task/projected-database references and allocation rate, not by missing queue synchronization.
- A queued task retains its parent projected transaction list, `itemsToKeep`, and prefix. Increasing the outstanding limit can therefore increase live memory even though tasks do not copy the whole database.

## Known issues and next experiments

- Forced process termination (SIGKILL, IDE force-stop, power loss) cannot execute `finally`; leftover partition directories may remain in that case. Cleanup is permanent deletion of reproducible temporary files, not trash. Error-path cleanup is implemented but was not fault-injection tested in this bounded validation.

- Sequential batch runs reset mining state but share one JVM: JIT compilation, garbage-collector history and OS filesystem caches are not reset. Use separate JVM launches for cold-process benchmark comparisons; do not interpret batch reset as a cold-cache guarantee.

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
15. The single `wad` flag controls both partition order and candidate-queue order, as requested. Therefore `wad0` versus `wad1` measures the complete Work-aware SU policy rather than decomposing the two levels.
16. Partition work is estimated before root-level item filtering, so `transactionCount + sumSuffixLength` is conservative. Validate its ranking across datasets before generalizing the Accidents result.
17. Threshold tracing can produce millions of CSV rows. Its event queue is intentionally lossless and unbounded, so monitor writer lag and heap use on full datasets; never compare traced elapsed time directly with an untraced run.
18. The convergence tool downsamples each monotone trace to a configurable horizontal resolution (800 points by default). Use a larger `--points` value for unusually wide publication figures; the summary milestones are calculated from the full trace rather than the downsampled curve.
19. Object-free projection removes wrapper objects but not projected-database
    descriptors. Each live child still owns a primitive descriptor array, and
    dynamic growth can temporarily retain old arrays until GC. Compare `ofp0`/`ofp1`
    with identical queue limits using external peak RSS, allocation rate, and
    GC logs; phase-boundary `Max memory MB` sampling can miss hot-path peaks.
20. Removing transaction merging can increase projected occurrence count and
    scan work on datasets with many identical suffixes. Both object modes now
    run without merging so the representation ablation is fair, but compare
    against the former merging baseline before claiming total improvement.
21. The full SUSY 1 GB result shows that segmented descriptors do not solve the
    fundamental live-set problem: dense DFS levels retain enough 24-byte
    descriptors to fill the heap, while block lookup adds hot-loop indirection.
    Restore the merging baseline or redesign the search so it does not
    materialize every projected occurrence.
22. Root zero-copy alone removes only one descriptor layer and previously
    still OOMed at partition 89/99. The new `mbp64/q16` replay configuration
    completed full SUSY under 1 GB, but this does not prove other budgets,
    queue widths, worker counts, or datasets cannot exhaust non-projection
    memory.
23. Further block-size/capacity tuning cannot solve the former deep live-set
    case by itself. Keep lazy replay or another non-materializing fallback when
    evaluating wider frontiers; otherwise ancestor/child descriptor chains can
    reproduce the OOM.
24. The earlier 8-worker SUSY run OOMed with a larger 1504 MB heap at partition
    63/99 using `q=1024` and no projection budget. The successful 1 GB run used
    `q=16`, WAD, and MBP together, so it does not isolate which share of the
    improvement came from frontier width versus replay. Run a controlled
    budget/queue ablation in separate JVMs with diagnostics/tracing disabled.
25. `SUSY_Utility.txt` is not a conventional set-valued HUI database: item 0
    occurs from line 1 and duplicate item identifiers occur from line 2. The
    base PTM miner uses 0 as the unrenamed-item sentinel and projects duplicate
    items with `Arrays.binarySearch`, while the independent TKEH oracle follows
    different occurrence semantics. Consequently baseline PTM DFS already
    differs from the oracle. Do not make correctness claims on this file until
    the dataset generator assigns positive, disjoint item identifiers or a
    single canonical duplicate-normalization policy is applied to every scan.
26. Memory-bounded projection is a descriptor budget, not a whole-heap
    guarantee. The root partition and preprocessing structures must still fit
    in memory. The runner's automatic quarter-heap policy is capped at 512 MB
    to preserve headroom and prevent large heaps from effectively disabling
    lazy replay; it is a default heuristic, not a proven optimum.
27. Lazy projection replay can repeatedly traverse an ancestor chain and may
    become much slower at deep nodes. Sweep budgets in separate JVMs and report
    both peak RSS/GC and elapsed time; the 1 MB validation intentionally forces
    the fallback and is not a recommended production setting.
28. The per-partition budget is intentionally cumulative: descriptor blocks
    made unreachable during the partition are not credited back. This is
    conservative and avoids unsafe parallel ownership accounting, but can turn
    later candidates lazy even after GC has reclaimed earlier branches.
29. Explicit task-reference cleanup prevents at most the last completed task
    on each idle worker from pinning a projection chain. It makes objects
    collectible but does not make the JVM collect them immediately; if 10 GB
    runs still retain several gigabytes, use GC logs before considering
    reference-counted block reuse or a partition-boundary collection policy.
30. Full SUSY Top-2000 with 8 workers and a 600 MB heap still OOMs while loading
    root partition 77/105 after the headroom-aware MBP change. Lazy replay only
    bounds projected children; the `List<Transaction>` root representation can
    itself exceed the remaining heap. A hard low-memory guarantee now requires
    either packed root-partition storage or chunked/disk-backed root scanning.
    The OOM logger must not allocate concatenated diagnostic strings while the
    heap is exhausted; diagnostics now print expected transaction/file size
    before loading and preserve the original allocation stack.
## Change log

### 2026-10-04

- Cleaned the main runner so all experiment choices are visible in one
  `IDE EXPERIMENT SETUP` block. Removed its command-line parsing for dataset,
  Top-k, transaction limit, OFP, MBP, and projection budget; retained the
  separate correctness verifier's CLI for automated validation. Added an
  explicit automatic-versus-fixed MBP budget switch and replaced independent
  parallel/MBP/WAD booleans with `SERIAL_BASELINE`, `PARALLEL_MBP`, and
  `PARALLEL_MBP_WAD` presets. The current preset is `SERIAL_BASELINE`; shared
  settings remain OFP on, `p8` by default when parallel, `q=16`, and
  diagnostics on. Added compact CLI selection: mode codes 1/2/3 choose those
  presets and a second optional argument overrides the worker count, without
  reintroducing the former long command-line configuration.

### 2026-10-03

- Tested and removed indexed-lazy parent-ordinal projection. Although it
  reduced candidates and pure replay on a SUSY prefix, chained parent lookup
  increased elapsed time and did not reduce sampled heap. Removed its storage,
  cursor, split budget, API, runner/verifier flag, diagnostics, and `_ilp`
  filename token. Restored the full MBP allowance to descriptor blocks with
  direct pure-lazy fallback, then revalidated oracle = DFS = pool on Accidents
  first 1000/Top-1000 with a forced 1 MB budget.

### 2026-10-02

- Made the configured MBP cap subordinate to actual heap headroom after the
  root partition is loaded, retaining 20% of `Xmx` or 64 MB for other
  allocations and reporting the effective budget range. Correctness passed on
  the forced-lazy Accidents run. A full 600 MB SUSY retry advanced beyond the
  former descriptor failure but OOMed at root-partition loading, identifying
  the next memory bottleneck. Added pre-load diagnostics and removed
  allocation-heavy OOM message construction so future failures retain their
  original allocation site.

- Added deterministic post-task reference cleanup. Each asynchronous task now
  drops its parent projection, item array, prefix, and run reference in
  `finally` while preserving exactly-once pending accounting. The forced 1 MB
  lazy Accidents verifier still matched the independent oracle and exercised
  23,474 lazy projections. Full large-heap impact remains unmeasured.

- Replaced the runner's temporary fixed 2560 MB projection budget with an
  automatic quarter-heap policy bounded to 64--512 MB. Omitted/`auto` CLI
  budget values use the policy; numeric values remain explicit experiment
  overrides. Added `_mbpa<actualMB>` naming and header labeling, then verified
  the 1 GB and 10,000 MB heap mappings with successful smoke runs.

- Added optional memory-bounded lazy projection replay with no disk spill.
  Candidates reserve worst-case object-free descriptor blocks before building;
  budget overflow creates a replayable parent/extension view instead. Added
  runner/verifier flags, `_mbp<MB>` active log naming, and a diagnostic lazy
  projection count. A forced 1 MB Accidents first-1000 test exercised 23,474
  lazy nodes and matched the independent oracle in DFS and pool modes; the
  disabled baseline also passed. A full 5-million-row SUSY stress run then
  completed with `-Xmx1g`, 8 workers, `q=16`, and a 64 MB descriptor budget in
  176,950 ms at 869.11 MB sampled peak heap, exercising 10,131 lazy nodes.
  This resolves the observed descriptor OOM for that configuration only;
  SUSY's duplicate/zero-item semantics still prevent a correctness claim.

- Tested and removed adaptive Conditional Pair-Suffix Pruning. It reduced the
  bounded Accidents candidate count but did not produce a convincing runtime
  or massive-data benefit, and duplicate items in SUSY required additional
  special-case handling. Restored the original LU/SU search, runner/verifier
  interfaces, diagnostics, and log naming.

### 2026-10-01

- Tested and removed the `ParentWorkGroup` ownership/group-draining experiment.
  Cooperative sibling draining disrupted the global candidate WAD order and
  did not provide a useful overall tradeoff. Restored the single-candidate task
  queue, original queue-admission semantics, runner/verifier interfaces, and
  log naming while retaining object-free projection and root zero-copy.

- Recorded the failed SUSY 1504 MB candidate-pool run (`p8/wad0/ofp1/q1024`).
  It reached partition 63/99 rapidly but OOMed in worker-side child projection,
  showing that the broad retained-parent frontier plus concurrent projection
  construction overwhelms the larger heap. Added a small-queue/worker sweep as
  the immediate experiment before a scheduler redesign.

- Recorded the full 1 GB SUSY rerun after root zero-copy. It progressed to
  partition 89/99 but still failed in a depth-three child descriptor allocation
  after about 641 seconds. Root duplication is no longer the limiting layer;
  the remaining blocker is the live projected-descendant chain.

- Added zero-copy object-free root projection. The root now indexes the loaded
  partition list directly and allocates neither a three-long descriptor per
  root occurrence nor a copied transaction-reference array; child projections
  retain the segmented descriptor format.
  Compiled to `out/production/iTP` and passed independent-oracle checks in DFS
  and pool modes through Accidents first 1000/Top-1000 (`minUtil=67644`).

- Rejected object-free/no-merge projection on full SUSY/Top-1000 with a fixed
  1 GB heap. After eliminating contiguous payload copies with 64-occurrence
  blocks, the run still OOMed allocating a new block and was slower according
  to the user. The failure is excessive live descriptor volume plus block
  access overhead, not the former grow-copy policy.

### 2026-09-30

- Added optional object-free projected databases (`ofp`). Each occurrence is
  represented by one three-long descriptor (packed transaction-id/offset,
  prefix utility, remaining utility) over immutable partition transactions. Added an
  object-wrapper/no-merge ablation, runner/verifier flags, `_ofp` log naming,
  and removed transaction merging from the active mining path. Both modes pass
  independent-oracle checks on bounded Accidents prefixes.
- Removed the rejected ECTP and intra-candidate parallel-projection code from
  the active source; their historical experiment notes remain below.

- Added optional work-triggered intra-candidate parallel projection. Massive
  candidate scans can lend bounded transaction chunks to idle candidate workers;
  chunks use private projection/bound workspaces and are reduced in source
  order to retain transaction-merging and exact-utility behavior. Added
  runner/verifier flags, `_pp/_pw/_pc` log tokens, diagnostics, and idle-worker
  wake-up coordination. A forced-path Accidents oracle comparison passed; full
  massive-dataset performance remains unmeasured.

- Added optional Eager Cooperative Top-k Publication (ECTP): one atomic offer
  slot per mining thread, CAS combiner ownership, exact global-heap updates,
  bounded backpressure, partition/shutdown flushes, runner/verifier flags,
  `_ectp` log naming, and diagnostic lock/batch counters. It passed independent
  oracle checks on Accidents prefixes through 5000 transactions; initial batch
  occupancy is low, so performance benefit remains unproven.
- Removed the rejected fused-sibling and MCB implementations completely,
  including their APIs, runner/verifier flags, log tokens, workspaces, task
  variants, and diagnostics. Historical correctness and performance evidence
  is retained below; neither experiment remains selectable in the source.
- Recorded the full Accidents/Top-10000 MCB ablation. MCB preserved the final
  threshold/results and cut processed candidates by 7.7%, but took 106,745 ms
  versus 105,684 ms and increased sampled heap by 17.7%. Changed the main
  runner default back to `mcb0`; the implementation was later removed.
- Added optional two-level max-child bound pruning. The parallel scheduler can
  compute `max(u(Px), max_y SU(Pxy))` for borderline non-root siblings and
  discard a whole `Px` subtree before publishing its task. Added a hard
  per-worker matrix-cell cap, `mcb` runner/verifier flags and log token, and
  evaluation/pruning/skip/time diagnostics. Compiled and passed independent
  oracle comparisons on Accidents prefixes of 100, 1000, and 5000
  transactions. The first bounded timing was slightly negative, so this remains
  experimental rather than recommended.
- Recorded the user-run full Accidents/Top-10000 ablation. `fsp1/fb8` preserved
  the final threshold/results and reduced candidate/task counts, but took
  156,081 ms versus 105,684 ms and increased sampled heap from 3346.0 MB to
  3796.5 MB. The implementation was later removed as a rejected performance
  direction rather than retained as a contribution.
- Added optional parallel fused sibling projection with a configurable fixed
  sibling batch size. Each queued batch scans its shared parent once, constructs
  all sibling projections, aggregates exact utility and LU/SU, and publishes
  surviving descendants through the existing bounded scheduler. Added
  runner/verifier flags, `_fsp`/`_fb` log tokens, configuration/stats output,
  and diagnostic fused-batch/candidate counts. Fused mode forces WAD off.
  Compiled and validated exact results on Accidents prefixes of 100 and 1000
  transactions against both independent TKEH and PTM DFS; no full database was
  run.

### 2026-09-13

- Added run-level automatic deletion of the owned PTM partition directory after successful mining or Java exceptions. Replaced recursive `File` traversal with a no-symlink-following file-tree walk; preserved primary failures when cleanup also fails. Verified independent-oracle correctness and five small batch runs with the partition directory absent afterward; retained all result files.
- Updated the experiment main to run a configurable Top-k array sequentially under one shared configuration, with optional comma-separated CLI values. Each iteration creates a fresh miner, closes run-specific logging/tracing resources and restores global console/exception-handler state. Added millisecond timestamps and collision-safe output directories. Validated five runs on ten Accidents transactions and a bounded independent-oracle comparison; preserved the current WAD-off configuration.

### 2026-09-11

- Added `tools/plot_minutil_convergence.py`, a standard-library-only streaming chart tool for large threshold traces. It validates monotonic traces, defaults to a standalone LaTeX PGFPlots paper figure (with optional SVG), writes compact sidecar CSV series consumed by `\addplot table`, plots absolute `minUtil` against a common elapsed-time axis, reports convergence milestones, marks actual completion, and carries a completed run's final threshold to the longest shared time as requested.
- Moved per-run output directories under the repository's `result/` directory as requested; log and threshold CSV naming inside each run directory is unchanged.
- Changed experiment output to one directory per run, named with the existing log stem. Each directory contains the normal log and a threshold CSV. Added lossless asynchronous tracing for every strict `minUtil` increase and every actually processed partition start, including elapsed/wall timestamps, event source, partition sequence/item, and current threshold. Added an optional verifier trace-path argument and validated a first-100 Accidents run against the oracle; no full database was run locally.
- Extended the single `workAwarePriority`/`wad` flag to initial partition selection. `wad1` orders mineable partitions by `partitionSU / (transactionCount + sumSuffixLength)` and candidates by `SU / estimatedWork`; `wad0` restores average-transaction-utility partition order and FIFO candidate scheduling. A temporary separate `pwad` flag was removed at the user's request. Compiled all sources and validated both flag values on Accidents first 1000/Top-1000 against the independent oracle; both returned 1000 patterns with `minUtil=67644`. No full database was run locally.

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

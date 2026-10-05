/*
 * PTM-style baseline converted from EFIM-style code.
 * Purpose:
 *   - Build prefix-based suffix partitions by scanning the reduced DB once.
 *   - Load each partition fully into RAM.
 *   - Mine each partition using a candidate-level worker pool.
 *   - Optional descriptor budgeting falls back to lazy projection replay;
 *     it does not swap projected data to disk.
 *
 * Input format: items : transactionUtility : utilities
 * Example: 1 2 3 : 30 : 5 10 15
 */


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public class AlgoEFIM_PTMStyleBaseline {

    private int topK;
    private volatile long minUtil;
    private PriorityQueue<TopKPattern> topKQueue;
    private long thresholdAfterSingles;
    private long startTimestamp;
    private long endTimestamp;
    private double maximumMemoryUsageMb;
    private File thresholdTraceFile;
    private ThresholdTraceLogger thresholdTraceLogger;
    private volatile int currentPartitionIndex = -1;
    private volatile int currentPartitionItem = -1;

    private long patternCount;
    // Fixed defaults used by the experiment runner. The mechanisms remain in
    // the algorithm; main does not need to pass every switch on every run.
    private boolean activateSubtreeUtilityPruning = true;
    private boolean activateCandidateParallelism;
    private boolean activateWorkAwarePriority;
    private boolean activateObjectFreeProjection = true;
    private boolean activateMemoryBoundedProjection;
    private int projectionBudgetMegabytes = 256;
    private volatile ProjectionBudget currentProjectionBudget;
    private long minimumEffectiveProjectionBudgetBytes;
    private long maximumEffectiveProjectionBudgetBytes;
    private long zeroEffectiveProjectionBudgetPartitions;
    private boolean activateTransactionUtilityRaising = true;
    private boolean activateGlobalPairUtilityRaising = true;
    private int candidateWorkerCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    private int minimumTransactionsPerCandidateTask = 512;
    private boolean diagnosticStatistics;
    private final LongAdder diagnosticCandidateCount = new LongAdder();
    private final LongAdder diagnosticAsyncTaskCount = new LongAdder();
    private final LongAdder diagnosticSmallDfsSwitchCount = new LongAdder();
    private final LongAdder diagnosticQueueOverflowInlineCount = new LongAdder();
    private final LongAdder diagnosticLazyProjectionCount = new LongAdder();
    private int maximumOutstandingCandidateTasks = 128;
    private long serialDfsPartitionCount;
    private long candidatePoolPartitionCount;

    private long[] utilityBinArrayLU;
    private long[] utilityBinArraySU;

    private int[] oldNameToNewNames;
    private int[] newNamesToOldNames;
    private int newItemCount;
    private static final int IO_BUFFER = 1 << 13;

    private File partitionDir;
    private boolean ownsPartitionDir;

    public void configureCandidateParallelism(boolean enabled,
                                              int workerCount,
                                              boolean workAwarePriority) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be greater than 0");
        }
        this.activateCandidateParallelism = enabled;
        this.candidateWorkerCount = workerCount;
        this.activateWorkAwarePriority = workAwarePriority;
    }

    public void configureTransactionUtilityRaising(boolean enabled) {
        this.activateTransactionUtilityRaising = enabled;
    }

    /**
     * Build a global two-dimensional exact pair-utility matrix during the
     * phase-1 database scan and offer every observed pair before partitioning.
     * The matrix is released immediately after threshold raising.
     */
    public void configureGlobalPairUtilityRaising(boolean enabled) {
        this.activateGlobalPairUtilityRaising = enabled;
    }

    public void configureCandidateTaskLimit(int maximumOutstandingTasks) {
        if (maximumOutstandingTasks <= 0) {
            throw new IllegalArgumentException(
                    "maximumOutstandingTasks must be greater than 0");
        }
        this.maximumOutstandingCandidateTasks = maximumOutstandingTasks;
    }

    public void configureCandidateTaskMinimumTransactions(int minimumTransactions) {
        if (minimumTransactions <= 0) {
            throw new IllegalArgumentException(
                    "minimumTransactions must be greater than 0");
        }
        this.minimumTransactionsPerCandidateTask = minimumTransactions;
    }

    public void configureObjectFreeProjection(boolean enabled) {
        this.activateObjectFreeProjection = enabled;
    }

    /**
     * Cap newly materialized object-free projection descriptors per partition.
     * Once the budget is exhausted, descendants become replayable lazy views
     * over their parent instead of allocating more descriptor blocks.
     */
    public void configureMemoryBoundedProjection(boolean enabled,
                                                 int budgetMegabytes) {
        if (budgetMegabytes <= 0) {
            throw new IllegalArgumentException(
                    "projection budget must be greater than 0 MB");
        }
        this.activateMemoryBoundedProjection = enabled;
        this.projectionBudgetMegabytes = budgetMegabytes;
    }

    /** Diagnostic counters add hot-path overhead; keep disabled for timed runs. */
    public void configureDiagnosticStatistics(boolean enabled) {
        this.diagnosticStatistics = enabled;
    }

    public void configureThresholdTrace(File outputFile) {
        this.thresholdTraceFile = outputFile;
    }

    public void closeThresholdTrace() throws IOException, InterruptedException {
        if (thresholdTraceLogger != null) {
            thresholdTraceLogger.close();
            thresholdTraceLogger = null;
        }
    }

    public Itemsets runAlgorithm(int requestedK,
                                 String inputPath,
                                 String outputPath,
                                 int maximumTransactionCount) throws IOException, InterruptedException {
        partitionDir = null;
        ownsPartitionDir = false;
        Throwable failure = null;
        try {
            return runAlgorithmInternal(requestedK, inputPath, outputPath, maximumTransactionCount);
        } catch (IOException | InterruptedException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            if (ownsPartitionDir) {
                try {
                    deleteDirectory(partitionDir);
                    ownsPartitionDir = false;
                    System.out.println("[PARTITION-CLEANUP] Deleted temporary directory: "
                            + partitionDir.getAbsolutePath());
                } catch (IOException cleanupError) {
                    if (failure != null) failure.addSuppressed(cleanupError);
                    else throw cleanupError;
                }
            }
        }
    }

    private Itemsets runAlgorithmInternal(int requestedK,
                                          String inputPath,
                                          String outputPath,
                                          int maximumTransactionCount) throws IOException, InterruptedException {

        if (requestedK <= 0) {
            throw new IllegalArgumentException("k must be greater than 0");
        }
        if (activateMemoryBoundedProjection && !activateObjectFreeProjection) {
            throw new IllegalStateException(
                    "memory-bounded projection requires object-free projection");
        }
        this.topK = requestedK;
        // TKEH Algorithm 4 starts at 1 for positive-utility databases.
        // This also prevents nonexistent zero-utility itemsets from entering
        // the result when k is larger than the number of observed patterns.
        this.minUtil = 1L;
        this.topKQueue = new PriorityQueue<>(requestedK, TopKPattern.WORST_FIRST);
        this.thresholdAfterSingles = 0L;
        patternCount = 0;
        diagnosticCandidateCount.reset();
        diagnosticAsyncTaskCount.reset();
        diagnosticSmallDfsSwitchCount.reset();
        diagnosticQueueOverflowInlineCount.reset();
        diagnosticLazyProjectionCount.reset();
        currentProjectionBudget = null;
        minimumEffectiveProjectionBudgetBytes = Long.MAX_VALUE;
        maximumEffectiveProjectionBudgetBytes = 0L;
        zeroEffectiveProjectionBudgetPartitions = 0L;
        serialDfsPartitionCount = 0L;
        candidatePoolPartitionCount = 0L;
        maximumMemoryUsageMb = 0D;
        startTimestamp = System.currentTimeMillis();
        currentPartitionIndex = -1;
        currentPartitionItem = -1;
        if (thresholdTraceFile != null) {
            thresholdTraceLogger = new ThresholdTraceLogger(
                    thresholdTraceFile,
                    startTimestamp
            );
            traceEvent("RUN_START", "INITIAL", minUtil);
        }
        MemoryLogger.getInstance().reset();
        MemoryLogger.getInstance().checkMemory();

        // ==========================
        // PHASE 1: TWU + exact singleton/global-pair utilities
        // ==========================
        Phase1Stats stats = phase1ScanStats(inputPath, maximumTransactionCount);
        // The global pair matrix is still resident here, so this sample
        // captures its contribution before it is released.
        MemoryLogger.getInstance().checkMemory();
        utilityBinArrayLU = stats.twu;

        // RIU strategy: exact utilities of all 1-itemsets initialize top-k.
        for (int item = 1; item < stats.singletonUtility.length; item++) {
            if (stats.support[item] > 0) {
                offerTopK(new int[]{item}, stats.singletonUtility[item]);
            }
        }
        thresholdAfterSingles = minUtil;
        System.out.println("[TOP-K] k=" + topK
                + " | threshold after single items=" + thresholdAfterSingles);

        if (activateGlobalPairUtilityRaising) {
            raiseThresholdFromGlobalPairUtilities(stats);
            stats.clearGlobalPairUtilities();
        }

        if (activateTransactionUtilityRaising) {
            raiseThresholdFromTransactionUtilities(stats);
        }
        stats.clearTransactionCertificates();

        List<Integer> itemsToKeep = new ArrayList<>();
        for (int item = 1; item < utilityBinArrayLU.length; item++) {
            if (utilityBinArrayLU[item] >= minUtil) {
                itemsToKeep.add(item);
            }
        }

        insertionSort(itemsToKeep, utilityBinArrayLU);

        oldNameToNewNames = new int[stats.maxItem + 1];
        newNamesToOldNames = new int[itemsToKeep.size() + 1];

        int currentName = 1;
        for (int j = 0; j < itemsToKeep.size(); j++) {
            int oldItem = itemsToKeep.get(j);
            oldNameToNewNames[oldItem] = currentName;
            newNamesToOldNames[currentName] = oldItem;
            itemsToKeep.set(j, currentName);
            currentName++;
        }

        newItemCount = itemsToKeep.size();
        utilityBinArraySU = new long[newItemCount + 1];

        // ==========================
        // PHASE 2: PTM-style build prefix partitions
        // ==========================
        initPartitionDir(outputPath, inputPath);

        System.out.println("[PTM-STYLE] Building prefix partitions...");
        PartitionInfo[] partitions = buildPrefixPartitions(inputPath, maximumTransactionCount);
        MemoryLogger.getInstance().checkMemory();

        // Reused while each partition is loaded for root LU/SU. The optional
        // global exact-pair matrix has already been released at this point.
        RootPairWorkspace rootPairWorkspace = new RootPairWorkspace(newItemCount);

        // First-level SU for deciding primary items.
        //calculateRootSubtreeUtilityFromPartitions(partitions);

        List<Integer> itemsToExplore = new ArrayList<>();
        if (activateSubtreeUtilityPruning) {
            for (Integer item : itemsToKeep) {
                if (utilityBinArraySU[item] >= minUtil) {
                    itemsToExplore.add(item);
                }
            }
        } else {
            itemsToExplore.addAll(itemsToKeep);
        }

        // Extend Work-aware SU to the partition frontier. The denominator is
        // the estimated partition scan work already collected while writing
        // partitions, so this adds no database pass. WAD-off retains the
        // original average-transaction-utility order as the baseline.
        if (activateWorkAwarePriority) {
            itemsToExplore.sort((left, right) ->
                    comparePartitionsBySUWork(left, right, partitions));
        } else {
            itemsToExplore.sort((left, right) -> {
                int byAverage = Double.compare(
                        averageTransactionUtility(partitions[right]),
                        averageTransactionUtility(partitions[left])
                );
                return byAverage != 0 ? byAverage : Integer.compare(left, right);
            });
        }


        // ==========================
        // PHASE 3: process each prefix partition fully in RAM
        // ==========================
        System.out.println("[PTM-STYLE] Mining partitions in RAM...");
        System.out.println("[MINING] mode="
                + executionModeName()
                + " | workers=" + (activateCandidateParallelism ? candidateWorkerCount : 1)
                + " | taskAdmission=" + candidateTaskAdmissionName()
                + " | minTaskTransactions=" + minimumTransactionsPerCandidateTask
                + " | globalPairUtilityRaising=" + activateGlobalPairUtilityRaising
                + " | objectFreeProjection=" + activateObjectFreeProjection
                + " | memoryBoundedProjection=" + activateMemoryBoundedProjection
                + (activateMemoryBoundedProjection
                ? "(" + projectionBudgetMegabytes + "MB)"
                : "")
                + " | workAwarePriority=" + activateWorkAwarePriority
                + " | partitionPriority="
                + (activateWorkAwarePriority
                ? "SU/estimatedWork"
                : "AVG_TU"));
        // The disabled mode is a genuine recursive depth-first traversal.
        // Candidate tasks and the shared priority pool exist only in parallel mode.
        CandidateScheduler candidateScheduler = activateCandidateParallelism
                ? new CandidateScheduler(candidateWorkerCount)
                : null;
        try {
            for (int i = 0; i < itemsToExplore.size(); i++) {
                System.out.println(i+"/"+itemsToExplore.size());
                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime startTime = LocalDateTime.now();
                System.out.println(startTime.format(formatter));
                Integer e = itemsToExplore.get(i);
                PartitionInfo p = partitions[e];
                if (p == null) {
                    continue;
                }

                // UOM-style partition pruning.  This check is repeated here
                // because minUtil may have risen while processing earlier parts.
                if (utilityBinArraySU[e] < minUtil) {
                    partitions[e] = null;
                    continue;
                }

                // A suffix-free partition contains only an already-seeded
                // singleton and has no file records to mine.
                if (p.transactionCount == 0) {
                    partitions[e] = null;
                    continue;
                }

                int jInKeep = itemsToKeep.indexOf(e);
                if (jInKeep < 0) {
                    continue;
                }

                currentPartitionIndex++;
                currentPartitionItem = newNamesToOldNames[e];
                traceEvent("PARTITION_START", "PARTITION", minUtil);

                List<Transaction> partitionTransactions;
                if (diagnosticStatistics) {
                    System.out.println("[PARTITION-LOAD] expectedTrans="
                            + p.transactionCount
                            + " | fileMB=" + p.file.length() / 1024 / 1024);
                }
                partitionTransactions = loadPartitionIntoRam(p.file, p.transactionCount);
                MemoryLogger.getInstance().checkMemory();

                calculateRootPairBounds(partitionTransactions, rootPairWorkspace);
                // With global pair raising, every exact pair was inserted once
                // after phase 1. Re-inserting it here would duplicate patterns
                // in the top-k heap and corrupt the result.
                if (!activateGlobalPairUtilityRaising) {
                    offerRootPairUtilities(e, rootPairWorkspace);
                }
                List<Integer> newItemsToKeep = new ArrayList<>();
                List<Integer> newItemsToExplore = new ArrayList<>();
                buildRootItemListsFromUIP(
                        jInKeep,
                        itemsToKeep,
                        rootPairWorkspace,
                        newItemsToKeep,
                        newItemsToExplore
                );

                // Singletons and all pairs were seeded before mining. With fewer
                // than two extensions this partition cannot produce a triple.
                if (newItemsToKeep.size() < 2) {
                    partitionTransactions.clear();
                    partitions[e] = null;
                    continue;
                }

                filterUnpromisingItems(partitionTransactions, newItemsToKeep);

                if (!newItemsToKeep.isEmpty()) {
                    currentProjectionBudget = activateMemoryBoundedProjection
                            ? createProjectionBudgetForLoadedPartition()
                            : null;
                    try {
                        ProjectedDatabase rootDatabase =
                                ProjectedDatabase.fromTransactions(
                                        partitionTransactions,
                                        activateObjectFreeProjection
                                );
                        List<Integer> rootItemsToExplore = activateSubtreeUtilityPruning
                                ? newItemsToExplore
                                : newItemsToKeep;
                        if (activateCandidateParallelism) {
                            candidatePoolPartitionCount++;
                            candidateScheduler.minePartition(
                                    e,
                                    rootDatabase,
                                    newItemsToKeep,
                                    rootItemsToExplore,
                                    rootPairWorkspace
                            );
                        } else {
                            serialDfsPartitionCount++;
                            minePartitionDepthFirst(
                                    e,
                                    rootDatabase,
                                    newItemsToKeep,
                                    rootItemsToExplore,
                                    rootPairWorkspace.su
                            );
                        }
                    } finally {
                        currentProjectionBudget = null;
                    }
                }

                MemoryLogger.getInstance().checkMemory();
                partitionTransactions.clear();
                partitionTransactions = null;
                partitions[e] = null;
            }
        } finally {
            if (candidateScheduler != null) {
                candidateScheduler.shutdown();
            }
        }



        endTimestamp = System.currentTimeMillis();
        MemoryLogger.getInstance().checkMemory();
        maximumMemoryUsageMb = MemoryLogger.getInstance().getMaxMemory();
        //printStats();
        patternCount = topKQueue.size();
        traceEvent("RUN_END", "FINAL", minUtil);
        closeThresholdTrace();
        return buildTopKResult();
    }



    private PartitionInfo[] buildPrefixPartitions(String inputPath, int maximumTransactionCount) throws IOException {
        PartitionInfo[] infos = new PartitionInfo[newItemCount + 1];
        DataOutputStream[] outs = new DataOutputStream[newItemCount + 1];

        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int count = 0;

            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '%' || line.charAt(0) == '@') {
                    continue;
                }
                count++;

                Transaction t = parseAndReduceTransaction(line);
                if (t != null && t.items.length > 0) {
                    long[] suffixUtility = new long[t.items.length];
                    long runningSuffix = 0L;
                    for (int pos = t.items.length - 1; pos >= 0; pos--) {
                        runningSuffix += t.utilities[pos];
                        suffixUtility[pos] = runningSuffix;
                    }

                    // PTM-style: for each item at position b, write suffix t[b..h] to partition P_item.
                    for (int pos = 0; pos < t.items.length; pos++) {
                        int item = t.items[pos];
//                        if(item<1876){
//                            continue;
//                        }
                        Transaction suffix = createSuffixExcludingPrefixItem(t, pos);


                        if (outs[item] == null) {
                            File f = new File(partitionDir, "P_" + item + ".bin");
                            outs[item] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f), IO_BUFFER));
                            infos[item] = new PartitionInfo(f);
                        }
                        suffix.prefixUtility = t.utilities[pos];
                        infos[item].occurrenceCount++;
                        infos[item].sumTransactionUtility += suffixUtility[pos];
                        utilityBinArraySU[item] += suffix.prefixUtility + suffix.transactionUtility;
                        if (suffix.items.length == 0) {
                            continue;
                        }

                        infos[item].subtreeUtility +=
                                suffix.prefixUtility + suffix.transactionUtility;
                        writeTransaction(outs[item], suffix);
                        infos[item].transactionCount++;
                        infos[item].sumLength += suffix.items.length;

                    }
                }

                if (count == maximumTransactionCount) {
                    break;
                }
            }
        } finally {
            for (DataOutputStream out : outs) {
                if (out != null) out.close();
            }
        }
        return infos;
    }

    private Transaction parseAndReduceTransaction(String line) {
        String[] split = line.split(":");
        long transactionUtility = Long.parseLong(split[1].trim());
        String[] itemStrings = split[0].trim().split(" ");
        String[] utilStrings = split[2].trim().split(" ");

        int keptCount = 0;
        int[] tempItems = new int[itemStrings.length];
        long[] tempUtils = new long[itemStrings.length];
        long newTU = transactionUtility;

        for (int i = 0; i < itemStrings.length; i++) {
            int oldItem = Integer.parseInt(itemStrings[i]);
            long utility = Long.parseLong(utilStrings[i]);
            int newItem = oldItem < oldNameToNewNames.length ? oldNameToNewNames[oldItem] : 0;

            if (newItem != 0) {
                tempItems[keptCount] = newItem;
                tempUtils[keptCount] = utility;
                keptCount++;
            } else {
                newTU -= utility;
            }
        }

        if (keptCount == 0) return null;

        int[] items = Arrays.copyOf(tempItems, keptCount);
        long[] utils = Arrays.copyOf(tempUtils, keptCount);
        Transaction.insertionSort(items, utils);
        return new Transaction(items, utils, newTU);
    }

    /**
     * PTM partition stores suffix INCLUDING the prefix item.
     * For t = [1,2,3], partition P2 stores [2,3].
     */
    private Transaction createSuffixExcludingPrefixItem(Transaction t, int pos) {
        int start = pos + 1;
        int len = t.items.length - start;

        int[] items = new int[len];
        long[] utils = new long[len];

        long tu = 0L;
        for (int i = 0; i < len; i++) {
            items[i] = t.items[start + i];
            utils[i] = t.utilities[start + i];
            tu += utils[i];
        }

        Transaction suffix = new Transaction(items, utils, tu);
        suffix.prefixUtility = t.prefixUtility + t.utilities[pos];
        suffix.offset = 0;
        return suffix;
    }

    private List<Transaction> loadPartitionIntoRam(File file, int expectedTrans) throws IOException {
        List<Transaction> list = new ArrayList<>(expectedTrans);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file), IO_BUFFER))) {

            while (true) {
                Transaction t = readTransactionOrNull(dis);
                if (t == null) break;

                list.add(t);

            }
        } catch (OutOfMemoryError oom) {
            // Do not build a diagnostic String while the heap is exhausted;
            // doing so can replace the useful original allocation stack with
            // a secondary OOM in String concatenation.
            System.err.println("[OOM WHILE LOAD PARTITION]");
            throw oom;
        }

        return list;
    }

    private double averageTransactionUtility(PartitionInfo partition) {
        if (partition == null || partition.occurrenceCount == 0) {
            return 0D;
        }
        return (double) partition.sumTransactionUtility / partition.occurrenceCount;
    }

    private int comparePartitionsBySUWork(int left,
                                          int right,
                                          PartitionInfo[] partitions) {
        PartitionInfo leftPartition = partitions[left];
        PartitionInfo rightPartition = partitions[right];
        boolean leftMineable = leftPartition != null
                && leftPartition.transactionCount > 0;
        boolean rightMineable = rightPartition != null
                && rightPartition.transactionCount > 0;
        if (leftMineable != rightMineable) {
            return leftMineable ? -1 : 1;
        }

        long leftSU = leftPartition == null ? 0L : leftPartition.subtreeUtility;
        long rightSU = rightPartition == null ? 0L : rightPartition.subtreeUtility;
        double leftScore = (double) leftSU
                / (double) estimatedPartitionWork(leftPartition);
        double rightScore = (double) rightSU
                / (double) estimatedPartitionWork(rightPartition);
        int byScore = Double.compare(rightScore, leftScore);
        if (byScore != 0) return byScore;

        int bySU = Long.compare(rightSU, leftSU);
        return bySU != 0 ? bySU : Integer.compare(left, right);
    }

    private long estimatedPartitionWork(PartitionInfo partition) {
        if (partition == null) return 1L;
        long transactionWork = partition.transactionCount;
        return partition.sumLength > Long.MAX_VALUE - transactionWork
                ? Long.MAX_VALUE
                : Math.max(1L, partition.sumLength + transactionWork);
    }

    /**
     * Build the root promising/explored lists from the precomputed UIP-style
     * metadata, before the partition file is loaded into memory.
     */
    private void buildRootItemListsFromUIP(int jInKeep,
                                           List<Integer> itemsToKeep,
                                           RootPairWorkspace pairBounds,
                                           List<Integer> newItemsToKeep,
                                           List<Integer> newItemsToExplore) {
        for (int index = jInKeep + 1; index < itemsToKeep.size(); index++) {
            int item = itemsToKeep.get(index);
            long lu = pairBounds.luValue(item);
            long su = pairBounds.suValue(item);

            if (su >= minUtil) {
                if (activateSubtreeUtilityPruning) {
                    newItemsToExplore.add(item);
                }
                newItemsToKeep.add(item);
            } else if (lu >= minUtil) {
                newItemsToKeep.add(item);
            }
        }
    }

    /**
     * Calculate root-pair LU/SU from the partition currently resident in RAM.
     * Arrays are reused for every partition and reset lazily through epochs.
     */
    private void calculateRootPairBounds(List<Transaction> partition,
                                         RootPairWorkspace workspace) {
        workspace.reset();
        for (Transaction transaction : partition) {
            long remainingUtility = 0L;
            for (int index = transaction.items.length - 1;
                 index >= transaction.offset; index--) {
                int item = transaction.items[index];
                workspace.touch(item);
                remainingUtility += transaction.utilities[index];
                workspace.exact[item] +=
                        transaction.prefixUtility + transaction.utilities[index];
                workspace.su[item] += transaction.prefixUtility + remainingUtility;
                workspace.lu[item] +=
                        transaction.prefixUtility + transaction.transactionUtility;
                workspace.suffixWork[item] += transaction.items.length - index;
            }
        }
    }

    private void offerRootPairUtilities(int rootItem,
                                        RootPairWorkspace workspace) {
        int oldRootItem = newNamesToOldNames[rootItem];
        for (int index = 0; index < workspace.touchedCount; index++) {
            int extension = workspace.touched[index];
            offerTopK(
                    new int[]{oldRootItem, newNamesToOldNames[extension]},
                    workspace.exact[extension]
            );
        }
    }

    /**
     * PTM unpromising-item filtering.  Only root-promising extension items
     * remain in RAM; prefix utility is kept separately on each transaction.
     */
    private void filterUnpromisingItems(List<Transaction> transactions,
                                        List<Integer> promisingItems) {
        boolean[] promising = new boolean[newItemCount + 1];
        for (Integer item : promisingItems) {
            promising[item] = true;
        }

        int outputIndex = 0;
        for (int inputIndex = 0; inputIndex < transactions.size(); inputIndex++) {
            Transaction transaction = transactions.get(inputIndex);
            int keptCount = 0;
            long keptUtility = 0L;

            for (int index = transaction.offset; index < transaction.items.length; index++) {
                if (promising[transaction.items[index]]) {
                    keptCount++;
                    keptUtility += transaction.utilities[index];
                }
            }

            if (keptCount == 0) {
                continue;
            }

            if (keptCount != transaction.items.length - transaction.offset) {
                int[] items = new int[keptCount];
                long[] utilities = new long[keptCount];
                int destination = 0;

                for (int index = transaction.offset; index < transaction.items.length; index++) {
                    if (promising[transaction.items[index]]) {
                        items[destination] = transaction.items[index];
                        utilities[destination] = transaction.utilities[index];
                        destination++;
                    }
                }

                transaction.items = items;
                transaction.utilities = utilities;
                transaction.offset = 0;
                transaction.transactionUtility = keptUtility;
            }

            transactions.set(outputIndex++, transaction);
        }

        while (transactions.size() > outputIndex) {
            transactions.remove(transactions.size() - 1);
        }
    }

    private int[] toIntArray(List<Integer> items) {
        int[] result = new int[items.size()];
        for (int i = 0; i < items.size(); i++) {
            result[i] = items.get(i);
        }
        return result;
    }

    private int[] appendItem(int[] prefix, int oldItem) {
        int[] result = Arrays.copyOf(prefix, prefix.length + 1);
        result[prefix.length] = oldItem;
        return result;
    }

    /**
     * Serial mode uses a real recursive DFS. It deliberately does not create
     * an executor, a candidate task, or a shared candidate frontier.
     */
    private void minePartitionDepthFirst(int rootItem,
                                         ProjectedDatabase rootTransactions,
                                         List<Integer> itemsToKeep,
                                         List<Integer> itemsToExplore,
                                         long[] rootSU) {
        int[] keep = toIntArray(itemsToKeep);
        int[] extensions = toIntArray(itemsToExplore);
        long[] subtreeUtilities = new long[extensions.length];
        for (int index = 0; index < extensions.length; index++) {
            subtreeUtilities[index] = rootSU[extensions[index]];
        }
        int[] rootPrefix = new int[]{newNamesToOldNames[rootItem]};
        WorkerBins bins = new WorkerBins(newItemCount);
        for (int index = 0; index < extensions.length; index++) {
            mineCandidateDepthFirst(
                    rootTransactions,
                    keep,
                    rootPrefix,
                    extensions[index],
                    subtreeUtilities[index],
                    true,
                    bins
            );
        }
    }

    private void mineCandidateDepthFirst(ProjectedDatabase parent,
                                         int[] itemsToKeep,
                                         int[] parentPrefix,
                                         int extension,
                                         long subtreeUtility,
                                         boolean utilityAlreadyOffered,
                                         WorkerBins bins) {
        if (diagnosticStatistics) diagnosticCandidateCount.increment();
        if (activateSubtreeUtilityPruning && subtreeUtility < minUtil) return;

        int positionInKeep = Arrays.binarySearch(itemsToKeep, extension);
        if (positionInKeep < 0) return;

        bins.reset();
        ParallelBuildResult projection = buildProjectedNodeReadOnly(
                parent,
                extension,
                bins
        );
        int[] currentPrefix = appendItem(
                parentPrefix,
                newNamesToOldNames[extension]
        );
        if (!utilityAlreadyOffered) {
            offerTopK(currentPrefix, projection.utility);
        }
        if (projection.transactions.isEmpty()) return;

        long currentThreshold = minUtil;
        int suffixSize = itemsToKeep.length - positionInKeep - 1;
        int[] keptBuffer = new int[suffixSize];
        int[] exploredBuffer = new int[suffixSize];
        int keptCount = 0;
        int exploredCount = 0;

        for (int index = positionInKeep + 1; index < itemsToKeep.length; index++) {
            int child = itemsToKeep[index];
            if (bins.suValue(child) >= currentThreshold) {
                keptBuffer[keptCount++] = child;
                if (activateSubtreeUtilityPruning) {
                    exploredBuffer[exploredCount++] = child;
                }
            } else if (bins.luValue(child) >= currentThreshold) {
                keptBuffer[keptCount++] = child;
            }
        }

        int[] newItemsToKeep = Arrays.copyOf(keptBuffer, keptCount);
        int[] children = activateSubtreeUtilityPruning
                ? Arrays.copyOf(exploredBuffer, exploredCount)
                : newItemsToKeep;
        long[] childSubtreeUtilities = new long[children.length];
        for (int index = 0; index < children.length; index++) {
            childSubtreeUtilities[index] = bins.suValue(children[index]);
        }
        for (int index = 0; index < children.length; index++) {
            mineCandidateDepthFirst(
                    projection.transactions,
                    newItemsToKeep,
                    currentPrefix,
                    children[index],
                    childSubtreeUtilities[index],
                    false,
                    bins
            );
        }
        projection.transactions.clear();
    }

    /**
     * Seed the shared frontier in the same order used by its priority queue.
     * DFS deliberately does not call this method.
     */
    private void sortCandidatesByDescendingSUWorkPriority(int[] candidates,
                                                          long[] subtreeUtilities,
                                                          long[] estimatedWorks) {
        if (!activateWorkAwarePriority) return;
        for (int index = 1; index < candidates.length; index++) {
            int candidate = candidates[index];
            long utility = subtreeUtilities[index];
            long work = estimatedWorks[index];
            int position = index - 1;
            while (position >= 0 && compareSUWorkPriority(
                    utility,
                    work,
                    subtreeUtilities[position],
                    estimatedWorks[position]
            ) > 0) {
                candidates[position + 1] = candidates[position];
                subtreeUtilities[position + 1] = subtreeUtilities[position];
                estimatedWorks[position + 1] = estimatedWorks[position];
                position--;
            }
            candidates[position + 1] = candidate;
            subtreeUtilities[position + 1] = utility;
            estimatedWorks[position + 1] = work;
        }
    }

    private int compareSUWorkPriority(long leftSU,
                                      long leftWork,
                                      long rightSU,
                                      long rightWork) {
        if (!activateWorkAwarePriority) {
            return Long.compare(leftSU, rightSU);
        }
        double leftScore = (double) leftSU / (double) Math.max(1L, leftWork);
        double rightScore = (double) rightSU / (double) Math.max(1L, rightWork);
        int byScore = Double.compare(leftScore, rightScore);
        return byScore != 0 ? byScore : Long.compare(leftSU, rightSU);
    }

    /** A reusable worker pool with exactly one candidate per queued task. */
    private final class CandidateScheduler {
        private final PriorityBlockingQueue<CandidateTask> candidateQueue =
                new PriorityBlockingQueue<>();
        private final Semaphore queueSlots;
        private final Thread[] workers;
        private final AtomicLong sequence = new AtomicLong();
        private volatile boolean stopped;
        private final ThreadLocal<WorkerBins> workerBins =
                ThreadLocal.withInitial(() -> new WorkerBins(newItemCount));

        CandidateScheduler(int workerCount) {
            queueSlots = new Semaphore(Math.max(
                    workerCount,
                    maximumOutstandingCandidateTasks
            ));
            workers = new Thread[workerCount];
            for (int index = 0; index < workerCount; index++) {
                Thread worker = new Thread(
                        this::workerLoop,
                        "candidate-worker-" + (index + 1)
                );
                workers[index] = worker;
                worker.start();
            }
        }

        private void workerLoop() {
            while (!stopped) {
                try {
                    CandidateTask task = candidateQueue.take();
                    releaseQueueAdmission();
                    task.run();
                } catch (InterruptedException interrupted) {
                    if (stopped) return;
                }
            }
        }

        void minePartition(int rootItem,
                           ProjectedDatabase rootTransactions,
                           List<Integer> itemsToKeep,
                           List<Integer> itemsToExplore,
                           RootPairWorkspace rootBounds) throws IOException, InterruptedException {
            PartitionRun run = new PartitionRun();
            int[] keep = toIntArray(itemsToKeep);
            int[] rootPrefix = new int[]{newNamesToOldNames[rootItem]};

            int[] extensions = toIntArray(itemsToExplore);
            long[] subtreeUtilities = new long[extensions.length];
            long[] estimatedWorks = new long[extensions.length];
            for (int index = 0; index < extensions.length; index++) {
                int extension = extensions[index];
                subtreeUtilities[index] = rootBounds.suValue(extension);
                estimatedWorks[index] = rootBounds.estimatedWorkValue(
                        extension,
                        rootTransactions.size()
                );
            }
            // Producer guard: prevents a very fast initial task from making
            // pending reach zero while the remaining initial tasks are added.
            run.pending.incrementAndGet();
            enqueueCandidates(
                    run,
                    rootTransactions,
                    keep,
                    rootPrefix,
                    extensions,
                    subtreeUtilities,
                    estimatedWorks,
                    true,
                    true
            );
            run.finishOne();
            run.completed.await();

            Throwable failure = run.failure.get();
            if (failure instanceof IOException) throw (IOException) failure;
            if (failure instanceof InterruptedException) throw (InterruptedException) failure;
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
            if (failure != null) throw new IOException("Candidate worker failed", failure);
        }

        private void enqueueCandidates(PartitionRun run,
                                       ProjectedDatabase parent,
                                       int[] itemsToKeep,
                                       int[] parentPrefix,
                                       int[] extensions,
                                       long[] subtreeUtilities,
                                       long[] estimatedWorks,
                                       boolean utilityAlreadyOffered,
                                       boolean initialCandidates) {
            int eligibleCount = 0;
            for (int index = 0; index < extensions.length; index++) {
                if (activateSubtreeUtilityPruning
                        && subtreeUtilities[index] < minUtil) {
                    continue;
                }
                extensions[eligibleCount] = extensions[index];
                subtreeUtilities[eligibleCount] = subtreeUtilities[index];
                estimatedWorks[eligibleCount] = estimatedWorks[index];
                eligibleCount++;
            }
            if (eligibleCount == 0) {
                return;
            }

            int[] eligibleExtensions = eligibleCount == extensions.length
                    ? extensions
                    : Arrays.copyOf(extensions, eligibleCount);
            long[] eligibleUtilities = eligibleCount == subtreeUtilities.length
                    ? subtreeUtilities
                    : Arrays.copyOf(subtreeUtilities, eligibleCount);
            long[] eligibleWorks = eligibleCount == estimatedWorks.length
                    ? estimatedWorks
                    : Arrays.copyOf(estimatedWorks, eligibleCount);
            if (initialCandidates) {
                sortCandidatesByDescendingSUWorkPriority(
                        eligibleExtensions,
                        eligibleUtilities,
                        eligibleWorks
                );
            }

            if (parent.size() < minimumTransactionsPerCandidateTask) {
                if (diagnosticStatistics) {
                    diagnosticSmallDfsSwitchCount.add(eligibleCount);
                }
                WorkerBins bins = workerBins.get();
                for (int index = 0; index < eligibleCount; index++) {
                    mineCandidateDepthFirst(
                            parent,
                            itemsToKeep,
                            parentPrefix,
                            eligibleExtensions[index],
                            eligibleUtilities[index],
                            utilityAlreadyOffered,
                            bins
                    );
                }
                return;
            }

            for (int index = 0; index < eligibleCount; index++) {
                enqueueCandidate(
                        run,
                        parent,
                        itemsToKeep,
                        parentPrefix,
                        eligibleExtensions[index],
                        eligibleUtilities[index],
                        eligibleWorks[index],
                        utilityAlreadyOffered
                );
            }
        }

        private void enqueueCandidate(PartitionRun run,
                                      ProjectedDatabase parent,
                                      int[] itemsToKeep,
                                      int[] parentPrefix,
                                      int extension,
                                      long subtreeUtility,
                                      long estimatedWork,
                                      boolean utilityAlreadyOffered) {
            if (!queueSlots.tryAcquire()) {
                // The fixed frontier is full. Continue synchronously without
                // allocating a task or touching the pending counter.
                if (diagnosticStatistics) {
                    diagnosticQueueOverflowInlineCount.increment();
                }
                processCandidate(
                        run,
                        parent,
                        itemsToKeep,
                        parentPrefix,
                        extension,
                        subtreeUtility,
                        utilityAlreadyOffered,
                        workerBins.get()
                );
                return;
            }

            CandidateTask task = new CandidateTask(
                    run,
                    parent,
                    itemsToKeep,
                    parentPrefix,
                    extension,
                    subtreeUtility,
                    estimatedWork,
                    utilityAlreadyOffered,
                    sequence.getAndIncrement()
            );
            run.pending.incrementAndGet();
            if (diagnosticStatistics) diagnosticAsyncTaskCount.increment();
            candidateQueue.offer(task);
        }

        private void releaseQueueAdmission() {
            queueSlots.release();
        }

        void shutdown() throws InterruptedException {
            stopped = true;
            for (Thread worker : workers) {
                worker.interrupt();
            }
            for (Thread worker : workers) {
                worker.join();
            }
        }

        private final class CandidateTask implements Runnable, Comparable<CandidateTask> {
            PartitionRun run;
            ProjectedDatabase parent;
            int[] itemsToKeep;
            int[] parentPrefix;
            final int extension;
            final long subtreeUtility;
            final long estimatedWork;
            final double priorityScore;
            final boolean utilityAlreadyOffered;
            final long sequenceNumber;

            CandidateTask(PartitionRun run,
                          ProjectedDatabase parent,
                          int[] itemsToKeep,
                          int[] parentPrefix,
                          int extension,
                          long subtreeUtility,
                          long estimatedWork,
                          boolean utilityAlreadyOffered,
                          long sequenceNumber) {
                this.run = run;
                this.parent = parent;
                this.itemsToKeep = itemsToKeep;
                this.parentPrefix = parentPrefix;
                this.extension = extension;
                this.subtreeUtility = subtreeUtility;
                this.estimatedWork = Math.max(1L, estimatedWork);
                this.priorityScore = (double) this.subtreeUtility
                        / (double) this.estimatedWork;
                this.utilityAlreadyOffered = utilityAlreadyOffered;
                this.sequenceNumber = sequenceNumber;
            }

            @Override
            public int compareTo(CandidateTask other) {
                if (activateWorkAwarePriority) {
                    int byScore = Double.compare(other.priorityScore, priorityScore);
                    if (byScore != 0) return byScore;
                    int bySU = Long.compare(other.subtreeUtility, subtreeUtility);
                    if (bySU != 0) return bySU;
                }
                return Long.compare(sequenceNumber, other.sequenceNumber);
            }

            @Override
            public void run() {
                PartitionRun activeRun = run;
                try {
                    if (activeRun.failure.get() != null) return;
                    WorkerBins bins = workerBins.get();
                    processCandidate(
                            activeRun,
                            parent,
                            itemsToKeep,
                            parentPrefix,
                            extension,
                            subtreeUtility,
                            utilityAlreadyOffered,
                            bins
                    );
                } catch (Throwable failure) {
                    activeRun.fail(failure);
                } finally {
                    // A worker can block in queue.take() while its last local
                    // CandidateTask remains stack-reachable. Break the heavy
                    // reference chain explicitly so a completed task cannot
                    // pin a projected database, lazy ancestors, or partition
                    // transactions until another task arrives.
                    parent = null;
                    itemsToKeep = null;
                    parentPrefix = null;
                    run = null;
                    activeRun.finishOne();
                }
            }
        }

        private void processCandidate(PartitionRun run,
                                      ProjectedDatabase parent,
                                      int[] itemsToKeep,
                                      int[] parentPrefix,
                                      int extension,
                                      long subtreeUtility,
                                      boolean utilityAlreadyOffered,
                                      WorkerBins bins) {
            if (diagnosticStatistics) diagnosticCandidateCount.increment();
            if (run.failure.get() != null) return;
            if (activateSubtreeUtilityPruning && subtreeUtility < minUtil) return;

            int positionInKeep = Arrays.binarySearch(itemsToKeep, extension);
            if (positionInKeep < 0) return;

            bins.reset();
            ParallelBuildResult projection = buildProjectedNodeReadOnly(
                    parent,
                    extension,
                    bins
            );
            int[] currentPrefix = appendItem(
                    parentPrefix,
                    newNamesToOldNames[extension]
            );

            if (!utilityAlreadyOffered) {
                offerTopK(currentPrefix, projection.utility);
            }

            if (projection.transactions.isEmpty()) {
                return;
            }

            long currentThreshold = minUtil;
            int suffixSize = itemsToKeep.length - positionInKeep - 1;
            int[] keptBuffer = new int[suffixSize];
            int[] exploredBuffer = new int[suffixSize];
            int keptCount = 0;
            int exploredCount = 0;

            for (int index = positionInKeep + 1;
                 index < itemsToKeep.length; index++) {
                int child = itemsToKeep[index];
                if (bins.suValue(child) >= currentThreshold) {
                    keptBuffer[keptCount++] = child;
                    if (activateSubtreeUtilityPruning) {
                        exploredBuffer[exploredCount++] = child;
                    }
                } else if (bins.luValue(child) >= currentThreshold) {
                    keptBuffer[keptCount++] = child;
                }
            }

            int[] newItemsToKeep = Arrays.copyOf(keptBuffer, keptCount);
            int[] children = activateSubtreeUtilityPruning
                    ? Arrays.copyOf(exploredBuffer, exploredCount)
                    : newItemsToKeep;

            // Queue-overflow DFS reuses and resets this thread's WorkerBins,
            // so snapshot every child bound before any child can run inline.
            long[] childSubtreeUtilities = new long[children.length];
            long[] childEstimatedWorks = new long[children.length];
            for (int index = 0; index < children.length; index++) {
                int child = children[index];
                childSubtreeUtilities[index] = bins.suValue(child);
                childEstimatedWorks[index] = bins.estimatedWorkValue(
                        child,
                        projection.transactions.size()
                );
            }

            if (children.length > 0) {
                enqueueCandidates(
                        run,
                        projection.transactions,
                        newItemsToKeep,
                        currentPrefix,
                        children,
                        childSubtreeUtilities,
                        childEstimatedWorks,
                        false,
                        false
                );
            }
        }
    }

    private static final class PartitionRun {
        final AtomicLong pending = new AtomicLong();
        final CountDownLatch completed = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        void fail(Throwable throwable) {
            failure.compareAndSet(null, throwable);
        }

        void finishOne() {
            if (pending.decrementAndGet() == 0L) {
                completed.countDown();
            }
        }
    }

    private static final class WorkerBins {
        final long[] lu;
        final long[] su;
        final long[] suffixWork;
        final int[] marks;
        final int[] touched;
        int epoch;
        int touchedCount;

        WorkerBins(int itemCount) {
            lu = new long[itemCount + 1];
            su = new long[itemCount + 1];
            suffixWork = new long[itemCount + 1];
            marks = new int[itemCount + 1];
            touched = new int[itemCount + 1];
        }

        void reset() {
            touchedCount = 0;
            epoch++;
            if (epoch == 0) {
                Arrays.fill(marks, 0);
                epoch = 1;
            }
        }

        void touch(int item) {
            if (marks[item] != epoch) {
                marks[item] = epoch;
                lu[item] = 0L;
                su[item] = 0L;
                suffixWork[item] = 0L;
                touched[touchedCount++] = item;
            }
        }


        long luValue(int item) {
            return marks[item] == epoch ? lu[item] : 0L;
        }

        long suValue(int item) {
            return marks[item] == epoch ? su[item] : 0L;
        }

        long estimatedWorkValue(int item, int parentTransactionCount) {
            long suffix = marks[item] == epoch ? suffixWork[item] : 0L;
            return parentTransactionCount + suffix;
        }
    }

    /**
     * Object-free projected database. The root reads metadata directly from
     * the immutable partition transactions; projected children create only
     * primitive descriptor entries, never per-occurrence Transaction objects.
     */
    private static final class ProjectedDatabase {
        private static final int DESCRIPTOR_BLOCK_SHIFT = 6;
        private static final int DESCRIPTORS_PER_BLOCK =
                1 << DESCRIPTOR_BLOCK_SHIFT;
        private static final int DESCRIPTOR_BLOCK_MASK =
                DESCRIPTORS_PER_BLOCK - 1;
        private static final int LONGS_PER_DESCRIPTOR = 3;
        final List<Transaction> baseTransactions;
        final boolean objectFree;
        final boolean rootView;
        final ProjectedDatabase lazyParent;
        final int lazyExtension;
        Transaction[] objectTransactions;
        long[][] descriptorBlocks;
        int descriptorBlockCount;
        int size;

        ProjectedDatabase(List<Transaction> baseTransactions,
                          boolean objectFree,
                          int initialCapacity) {
            this(baseTransactions, objectFree, false, initialCapacity);
        }

        private ProjectedDatabase(List<Transaction> baseTransactions,
                                  boolean objectFree,
                                  boolean rootView,
                                  int initialCapacity) {
            this(baseTransactions, objectFree, rootView, initialCapacity,
                    null, -1, 0);
        }

        private ProjectedDatabase(List<Transaction> baseTransactions,
                                  boolean objectFree,
                                  boolean rootView,
                                  int initialCapacity,
                                  ProjectedDatabase lazyParent,
                                  int lazyExtension,
                                  int lazySize) {
            this.baseTransactions = baseTransactions;
            this.objectFree = objectFree;
            this.rootView = rootView;
            this.lazyParent = lazyParent;
            this.lazyExtension = lazyExtension;
            if (lazyParent != null) {
                size = lazySize;
                return;
            }
            if (rootView) {
                size = baseTransactions.size();
                return;
            }
            int capacity = Math.max(1, initialCapacity);
            if (objectFree) {
                int requiredBlocks = blockCountFor(capacity);
                descriptorBlocks = new long[requiredBlocks][];
                for (int block = 0; block < requiredBlocks; block++) {
                    descriptorBlocks[block] = newDescriptorBlock();
                }
                descriptorBlockCount = requiredBlocks;
            } else {
                objectTransactions = new Transaction[capacity];
            }
        }

        static ProjectedDatabase lazy(ProjectedDatabase parent,
                                      int extension,
                                      int projectedSize) {
            return new ProjectedDatabase(
                    parent.baseTransactions,
                    true,
                    false,
                    0,
                    parent,
                    extension,
                    projectedSize
            );
        }

        static ProjectedDatabase fromTransactions(List<Transaction> transactions,
                                                  boolean objectFree) {
            if (objectFree) {
                // The partition transactions already contain the root offset,
                // prefix utility and remaining utility.  Read them directly
                // instead of materializing one redundant descriptor per row.
                return new ProjectedDatabase(
                        transactions,
                        true,
                        true,
                        0
                );
            }
            ProjectedDatabase database =
                    new ProjectedDatabase(
                            transactions,
                            false,
                            transactions.size()
                    );
            for (Transaction transaction : transactions) {
                database.addObject(transaction);
            }
            return database;
        }

        void addDescriptor(int transactionId,
                           int offset,
                           long prefixUtility,
                           long remainingUtility) {
            ensureCapacity(size + 1);
            long[] block = descriptorBlock(size);
            int descriptor = descriptorOffset(size);
            block[descriptor] = ((long) transactionId << 32)
                    | (offset & 0xffffffffL);
            block[descriptor + 1] = prefixUtility;
            block[descriptor + 2] = remainingUtility;
            size++;
        }

        void addObject(Transaction transaction) {
            ensureCapacity(size + 1);
            objectTransactions[size++] = transaction;
        }

        Transaction transactionAt(int index) {
            if (lazyParent != null) {
                throw new IllegalStateException(
                        "Random access is unavailable for a lazy projection");
            }
            if (rootView) return baseTransactions.get(index);
            return objectFree
                    ? baseTransactions.get(transactionIdAt(index))
                    : objectTransactions[index];
        }

        int transactionIdAt(int index) {
            if (lazyParent != null) {
                throw new IllegalStateException(
                        "Random access is unavailable for a lazy projection");
            }
            if (!objectFree) return -1;
            if (rootView) return index;
            return (int) (descriptorBlock(index)[descriptorOffset(index)] >>> 32);
        }

        int offsetAt(int index) {
            if (lazyParent != null) {
                throw new IllegalStateException(
                        "Random access is unavailable for a lazy projection");
            }
            if (rootView) return baseTransactions.get(index).offset;
            return objectFree
                    ? (int) descriptorBlock(index)[descriptorOffset(index)]
                    : objectTransactions[index].offset;
        }

        long prefixUtilityAt(int index) {
            if (lazyParent != null) {
                throw new IllegalStateException(
                        "Random access is unavailable for a lazy projection");
            }
            if (rootView) return baseTransactions.get(index).prefixUtility;
            return objectFree
                    ? descriptorBlock(index)[descriptorOffset(index) + 1]
                    : objectTransactions[index].prefixUtility;
        }

        long remainingUtilityAt(int index) {
            if (lazyParent != null) {
                throw new IllegalStateException(
                        "Random access is unavailable for a lazy projection");
            }
            if (rootView) return baseTransactions.get(index).transactionUtility;
            return objectFree
                    ? descriptorBlock(index)[descriptorOffset(index) + 2]
                    : objectTransactions[index].transactionUtility;
        }

        private void ensureCapacity(int required) {
            if (objectFree) {
                ensureDescriptorCapacity(required);
                return;
            }
            int currentCapacity = objectTransactions.length;
            if (required <= currentCapacity) return;
            int grown = currentCapacity + Math.max(1, currentCapacity >>> 1);
            int capacity = Math.max(required, grown);
            objectTransactions = Arrays.copyOf(objectTransactions, capacity);
        }

        private void ensureDescriptorCapacity(int required) {
            int requiredBlocks = blockCountFor(required);
            if (requiredBlocks <= descriptorBlockCount) return;
            if (requiredBlocks > descriptorBlocks.length) {
                int grown = descriptorBlocks.length
                        + Math.max(1, descriptorBlocks.length >>> 1);
                descriptorBlocks = Arrays.copyOf(
                        descriptorBlocks,
                        Math.max(requiredBlocks, grown)
                );
            }
            for (int block = descriptorBlockCount;
                 block < requiredBlocks; block++) {
                descriptorBlocks[block] = newDescriptorBlock();
            }
            descriptorBlockCount = requiredBlocks;
        }

        private long[] descriptorBlock(int index) {
            return descriptorBlocks[index >>> DESCRIPTOR_BLOCK_SHIFT];
        }

        private static int descriptorOffset(int index) {
            return (index & DESCRIPTOR_BLOCK_MASK) * LONGS_PER_DESCRIPTOR;
        }

        private static int blockCountFor(int descriptors) {
            return (descriptors + DESCRIPTORS_PER_BLOCK - 1)
                    >>> DESCRIPTOR_BLOCK_SHIFT;
        }

        private static long[] newDescriptorBlock() {
            return new long[DESCRIPTORS_PER_BLOCK * LONGS_PER_DESCRIPTOR];
        }

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        ProjectedCursor openCursor() {
            if (lazyParent != null) {
                return new LazyProjectedCursor(
                        lazyParent.openCursor(),
                        lazyExtension
                );
            }
            return new MaterializedProjectedCursor(this);
        }

        void clear() {
            if (lazyParent != null) {
                size = 0;
                return;
            }
            if (rootView) {
                size = 0;
                return;
            }
            if (!objectFree) {
                if (objectTransactions != null) {
                    Arrays.fill(objectTransactions, 0, size, null);
                    objectTransactions = null;
                }
            } else {
                descriptorBlocks = null;
                descriptorBlockCount = 0;
            }
            size = 0;
        }
    }

    private interface ProjectedCursor {
        boolean next();
        int transactionId();
        Transaction transaction();
        int offset();
        long prefixUtility();
        long remainingUtility();
    }

    private static final class MaterializedProjectedCursor
            implements ProjectedCursor {
        private final ProjectedDatabase database;
        private int index = -1;

        MaterializedProjectedCursor(ProjectedDatabase database) {
            this.database = database;
        }

        @Override
        public boolean next() {
            return ++index < database.size();
        }

        @Override
        public int transactionId() {
            return database.transactionIdAt(index);
        }

        @Override
        public Transaction transaction() {
            return database.transactionAt(index);
        }

        @Override
        public int offset() {
            return database.offsetAt(index);
        }

        @Override
        public long prefixUtility() {
            return database.prefixUtilityAt(index);
        }

        @Override
        public long remainingUtility() {
            return database.remainingUtilityAt(index);
        }
    }

    /** Replays one projection step without storing per-transaction descriptors. */
    private static final class LazyProjectedCursor implements ProjectedCursor {
        private final ProjectedCursor parent;
        private final int extension;
        private int transactionId;
        private Transaction transaction;
        private int offset;
        private long prefixUtility;
        private long remainingUtility;

        LazyProjectedCursor(ProjectedCursor parent, int extension) {
            this.parent = parent;
            this.extension = extension;
        }

        @Override
        public boolean next() {
            while (parent.next()) {
                Transaction candidate = parent.transaction();
                int parentOffset = parent.offset();
                int position = Arrays.binarySearch(
                        candidate.items,
                        parentOffset,
                        candidate.items.length,
                        extension
                );
                if (position < 0 || position == candidate.getLastPosition()) {
                    continue;
                }

                long newPrefixUtility = parent.prefixUtility()
                        + candidate.utilities[position];
                long newRemainingUtility = parent.remainingUtility()
                        - candidate.utilities[position];
                for (int index = parentOffset; index < position; index++) {
                    newRemainingUtility -= candidate.utilities[index];
                }

                transactionId = parent.transactionId();
                transaction = candidate;
                offset = position + 1;
                prefixUtility = newPrefixUtility;
                remainingUtility = newRemainingUtility;
                return true;
            }
            return false;
        }

        @Override
        public int transactionId() {
            return transactionId;
        }

        @Override
        public Transaction transaction() {
            return transaction;
        }

        @Override
        public int offset() {
            return offset;
        }

        @Override
        public long prefixUtility() {
            return prefixUtility;
        }

        @Override
        public long remainingUtility() {
            return remainingUtility;
        }
    }

    /**
     * Convert the configured descriptor cap into a safe per-partition cap
     * after the root partition and shared mining structures are resident.
     * The unused headroom protects non-projection allocations and gives the
     * collector room to operate on small heaps.
     */
    private ProjectionBudget createProjectionBudgetForLoadedPartition() {
        Runtime runtime = Runtime.getRuntime();
        long maxHeapBytes = runtime.maxMemory();
        long usedHeapBytes = runtime.totalMemory() - runtime.freeMemory();
        long safetyMarginBytes = Math.max(
                64L * 1024L * 1024L,
                maxHeapBytes / 5L
        );
        long availableForProjectionBytes = Math.max(
                0L,
                maxHeapBytes - usedHeapBytes - safetyMarginBytes
        );
        long configuredBytes = (long) projectionBudgetMegabytes
                * 1024L * 1024L;
        long effectiveBytes = Math.min(
                configuredBytes,
                availableForProjectionBytes
        );

        minimumEffectiveProjectionBudgetBytes = Math.min(
                minimumEffectiveProjectionBudgetBytes,
                effectiveBytes
        );
        maximumEffectiveProjectionBudgetBytes = Math.max(
                maximumEffectiveProjectionBudgetBytes,
                effectiveBytes
        );
        if (effectiveBytes == 0L) {
            zeroEffectiveProjectionBudgetPartitions++;
        }
        return new ProjectionBudget(effectiveBytes);
    }

    private static final class ProjectionBudget {
        // 1,536 payload bytes plus array/object/reference overhead.
        private static final long ESTIMATED_BYTES_PER_DESCRIPTOR_BLOCK = 1_664L;
        private final AtomicLong remainingBytes;

        ProjectionBudget(long bytes) {
            remainingBytes = new AtomicLong(Math.max(0L, bytes));
        }

        ProjectionReservation tryReserveDescriptors(int maximumDescriptors) {
            long blocks = ProjectedDatabase.blockCountFor(maximumDescriptors);
            return tryReserve(
                    remainingBytes,
                    blocks * ESTIMATED_BYTES_PER_DESCRIPTOR_BLOCK
            );
        }

        private ProjectionReservation tryReserve(AtomicLong account,
                                                 long bytes) {
            while (true) {
                long remaining = account.get();
                if (bytes > remaining) return null;
                if (account.compareAndSet(remaining, remaining - bytes)) {
                    return new ProjectionReservation(account, bytes);
                }
            }
        }
    }

    private static final class ProjectionReservation {
        private final AtomicLong account;
        private final long reservedBytes;

        ProjectionReservation(AtomicLong account, long reservedBytes) {
            this.account = account;
            this.reservedBytes = reservedBytes;
        }

        void keepAllocatedBytes(long allocatedBytes) {
            long unusedBytes = Math.max(0L, reservedBytes - allocatedBytes);
            if (unusedBytes > 0L) account.addAndGet(unusedBytes);
        }
    }

    private static final class ParallelBuildResult {
        final ProjectedDatabase transactions;
        final long utility;

        ParallelBuildResult(ProjectedDatabase transactions,
                            long utility) {
            this.transactions = transactions;
            this.utility = utility;
        }
    }

    private ParallelBuildResult buildProjectedNodeReadOnly(ProjectedDatabase parent,
                                                           int extension,
                                                           WorkerBins bins) {
        ProjectionReservation reservation = null;
        boolean materialize = true;
        ProjectionBudget budget = currentProjectionBudget;
        if (budget != null && parent.objectFree) {
            reservation = budget.tryReserveDescriptors(parent.size());
            materialize = reservation != null;
            if (!materialize && diagnosticStatistics) {
                diagnosticLazyProjectionCount.increment();
            }
        }
        ProjectedDatabase projectedTransactions = materialize
                ? new ProjectedDatabase(
                        parent.baseTransactions,
                        parent.objectFree,
                        Math.min(parent.size(), 64)
                )
                : null;
        long utility = 0L;
        int projectedSize = 0;
        ProjectedCursor cursor = parent.openCursor();
        while (cursor.next()) {
            int baseTransactionId = cursor.transactionId();
            Transaction transaction = cursor.transaction();
            int parentOffset = cursor.offset();
            long parentPrefixUtility = cursor.prefixUtility();
            long parentRemainingUtility = cursor.remainingUtility();
            int position = Arrays.binarySearch(
                    transaction.items,
                    parentOffset,
                    transaction.items.length,
                    extension
            );
            if (position < 0) continue;

            if (position == transaction.getLastPosition()) {
                utility += parentPrefixUtility + transaction.utilities[position];
                continue;
            }

            long projectedPrefixUtility =
                    parentPrefixUtility + transaction.utilities[position];
            long projectedRemainingUtility = parentRemainingUtility
                    - transaction.utilities[position];
            for (int index = parentOffset; index < position; index++) {
                projectedRemainingUtility -= transaction.utilities[index];
            }
            utility += projectedPrefixUtility;
            projectedSize++;

            long remainingUtility = 0L;
            for (int index = transaction.items.length - 1;
                 index > position; index--) {
                int item = transaction.items[index];
                bins.touch(item);
                remainingUtility += transaction.utilities[index];
                bins.su[item] += projectedPrefixUtility + remainingUtility;
                bins.lu[item] += projectedPrefixUtility
                        + projectedRemainingUtility;
                bins.suffixWork[item] += transaction.items.length - index;
            }
            if (!materialize) {
                continue;
            }
            if (parent.objectFree) {
                projectedTransactions.addDescriptor(
                        baseTransactionId,
                        position + 1,
                        projectedPrefixUtility,
                        projectedRemainingUtility
                );
            } else {
                projectedTransactions.addObject(
                        new Transaction(transaction, position)
                );
            }
        }

        if (!materialize) {
            projectedTransactions = ProjectedDatabase.lazy(
                    parent,
                    extension,
                    projectedSize
            );
        } else if (reservation != null) {
            long allocatedBytes = (long) projectedTransactions.descriptorBlockCount
                    * ProjectionBudget.ESTIMATED_BYTES_PER_DESCRIPTOR_BLOCK;
            reservation.keepAllocatedBytes(allocatedBytes);
        }

        return new ParallelBuildResult(
                projectedTransactions,
                utility
        );
    }

    // ============================================================
    // Binary transaction I/O
    // ============================================================

    private static void writeTransaction(DataOutputStream dos, Transaction transaction) throws IOException {
        int start = 0;
        int length = transaction.items.length - start;

        dos.writeInt(length);
        dos.writeLong(transaction.transactionUtility);
        dos.writeLong(transaction.prefixUtility);

        for (int i = start; i < transaction.items.length; i++) dos.writeInt(transaction.items[i]);
        for (int i = start; i < transaction.utilities.length; i++) dos.writeLong(transaction.utilities[i]);
    }

    private static Transaction readTransactionOrNull(DataInputStream dis) throws IOException {
        int length = -1;

        try {
            length = dis.readInt();
            long transactionUtility = dis.readLong();
            long prefixUtility = dis.readLong();

            int[] items;
            long[] utilities;

            try {
                items = new int[length];
            } catch (OutOfMemoryError oom) {
                printOOMRam("[OOM NEW int[]]", length);
                throw oom;
            }

            try {
                utilities = new long[length];
            } catch (OutOfMemoryError oom) {
                printOOMRam("[OOM NEW long[]]", length);
                throw oom;
            }

            for (int i = 0; i < length; i++) items[i] = dis.readInt();
            for (int i = 0; i < length; i++) utilities[i] = dis.readLong();

            Transaction transaction;

            try {
                transaction = new Transaction(items, utilities, transactionUtility);
            } catch (OutOfMemoryError oom) {
                printOOMRam("[OOM NEW Transaction]", length);
                throw oom;
            }

            transaction.prefixUtility = prefixUtility;
            transaction.offset = 0;

            return transaction;

        } catch (EOFException eof) {
            return null;
        }
    }

    private static void printOOMRam(String where, int length) {
        Runtime rt = Runtime.getRuntime();

        long used = rt.totalMemory() - rt.freeMemory();
        long committedFree = rt.freeMemory();
        long maxFree = rt.maxMemory() - used;

        System.err.println(
                where
                        + " length=" + length
                        + " usedMB=" + used / 1024 / 1024
                        + " committedFreeMB=" + committedFree / 1024 / 1024
                        + " maxFreeMB=" + maxFree / 1024 / 1024
                        + " totalMB=" + rt.totalMemory() / 1024 / 1024
                        + " maxMB=" + rt.maxMemory() / 1024 / 1024
        );
    }

    // ============================================================
    // Helpers
    // ============================================================

    private void initPartitionDir(String outputPath, String inputPath) throws IOException {
        String base = outputPath != null && !outputPath.trim().isEmpty() ? outputPath : inputPath;
        File baseFile = new File(base).getAbsoluteFile();
        File parent = baseFile.getParentFile() == null ? new File(".") : baseFile.getParentFile();
        partitionDir = new File(parent, baseFile.getName() +(Runtime.getRuntime().maxMemory()) +"10.ptm_partitions");
        deleteDirectory(partitionDir);
        if (!partitionDir.mkdirs() && !partitionDir.exists()) {
            throw new IOException("Cannot create partition dir: " + partitionDir.getAbsolutePath());
        }
        ownsPartitionDir = true;
    }

    private void deleteDirectory(File dir) throws IOException {
        if (dir == null || !Files.exists(dir.toPath(), LinkOption.NOFOLLOW_LINKS)) return;
        // Do not follow symbolic links out of the exact temporary partition directory.
        Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException error) throws IOException {
                if (error != null) throw error;
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private Phase1Stats phase1ScanStats(String inputPath, int maximumTransactionCount) throws IOException {
        int maxItem = 0;
        int minimumWitnessLength = activateGlobalPairUtilityRaising ? 3 : 2;
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '%' || line.charAt(0) == '@') continue;
                count++;
                String[] split = line.split(":");
                String[] items = split[0].trim().split(" ");
                for (String s : items) {
                    int item = Integer.parseInt(s);
                    if (item > maxItem) maxItem = item;
                }
                if (count == maximumTransactionCount) break;
            }
        }

        Phase1Stats stats = new Phase1Stats(
                maxItem,
                activateTransactionUtilityRaising ? topK : 0,
                activateGlobalPairUtilityRaising
        );
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#' || line.charAt(0) == '%' || line.charAt(0) == '@') continue;
                count++;
                String[] split = line.split(":");
                long tu = Long.parseLong(split[1].trim());
                String[] items = split[0].trim().split(" ");
                String[] utilities = split[2].trim().split(" ");
                stats.transactionCount++;
                // When global exact-pair raising is active, a two-item full
                // transaction denotes the same itemset already present in the
                // exact pair heap. Do not count it again as an independent
                // threshold witness. Longer transaction itemsets are distinct
                // from all seeded singletons and pairs.
                if (activateTransactionUtilityRaising
                        && items.length >= minimumWitnessLength) {
                    stats.offerTransactionCertificate(
                            transactionFingerprint(items),
                            tu
                    );
                }
                int[] transactionItems = activateGlobalPairUtilityRaising
                        ? new int[items.length]
                        : null;
                long[] transactionUtilities = activateGlobalPairUtilityRaising
                        ? new long[items.length]
                        : null;
                for (int i = 0; i < items.length; i++) {
                    int item = Integer.parseInt(items[i]);
                    long utility = Long.parseLong(utilities[i]);
                    stats.twu[item] += tu;
                    stats.support[item]++;
                    stats.singletonUtility[item] += utility;
                    if (activateGlobalPairUtilityRaising) {
                        transactionItems[i] = item;
                        transactionUtilities[i] = utility;
                    }
                }
                if (activateGlobalPairUtilityRaising) {
                    for (int left = 0; left < transactionItems.length; left++) {
                        int leftItem = transactionItems[left];
                        long leftUtility = transactionUtilities[left];
                        for (int right = left + 1;
                             right < transactionItems.length; right++) {
                            int rightItem = transactionItems[right];
                            int smaller = Math.min(leftItem, rightItem);
                            int greater = Math.max(leftItem, rightItem);
                            stats.globalPairUtility[smaller][greater] +=
                                    leftUtility + transactionUtilities[right];
                        }
                    }
                }
                if (count == maximumTransactionCount) break;
            }
        }
        return stats;
    }

    /** Offer each global exact 2-itemset once, then keep only the threshold. */
    private void raiseThresholdFromGlobalPairUtilities(Phase1Stats stats) {
        long[][] pairUtility = stats.globalPairUtility;
        if (pairUtility == null) return;

        // Use the same TWU order later used by the miner. Besides preserving
        // the prefix representation, this keeps deterministic tie-breaking at
        // the top-k boundary identical to root-pair insertion.
        List<Integer> pairOrder = new ArrayList<>();
        for (int item = 1; item < stats.support.length; item++) {
            if (stats.support[item] > 0L) pairOrder.add(item);
        }
        insertionSort(pairOrder, stats.twu);

        for (int leftIndex = 0; leftIndex < pairOrder.size(); leftIndex++) {
            int leftItem = pairOrder.get(leftIndex);
            for (int rightIndex = leftIndex + 1;
                 rightIndex < pairOrder.size(); rightIndex++) {
                int rightItem = pairOrder.get(rightIndex);
                int smaller = Math.min(leftItem, rightItem);
                int greater = Math.max(leftItem, rightItem);
                long utility = pairUtility[smaller][greater];
                if (utility >= minUtil) {
                    offerTopK(new int[]{leftItem, rightItem}, utility);
                }
            }
        }
        System.out.println("[TOP-K] threshold after global item pairs=" + minUtil);
    }

    /**
     * Order-independent fingerprint. Equal itemsets always have the same
     * fingerprint. A rare collision only discards an extra certificate, so it
     * can lower the starting threshold but cannot make pruning unsafe.
     */
    private long transactionFingerprint(String[] items) {
        long sum = 0x9e3779b97f4a7c15L ^ items.length;
        long xor = 0x632be59bd9b4e019L;
        for (String value : items) {
            long mixed = mix64(Long.parseLong(value));
            sum += mixed;
            xor ^= Long.rotateLeft(mixed, (int) (mixed & 63L));
        }
        long fingerprint = mix64(sum ^ Long.rotateLeft(xor, 23));
        return fingerprint == 0L ? 1L : fingerprint;
    }

    private static long mix64(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return value;
    }

    /**
     * Each full transaction itemset is a real, distinct pattern whose global
     * utility is at least its utility in that transaction (positive-utility
     * database).  The certificates raise only the pruning threshold; they are
     * never inserted into the exact result heap.
     */
    private void raiseThresholdFromTransactionUtilities(Phase1Stats stats) {
        PriorityQueue<Long> witnesses = new PriorityQueue<>(topK);

        for (TopKPattern singleton : topKQueue) {
            witnesses.offer(singleton.utility);
        }
        for (int index = 0; index < stats.transactionCertificates.size; index++) {
            witnesses.offer(stats.transactionCertificates.heapValues[index]);
            if (witnesses.size() > topK) {
                witnesses.poll();
            }
        }

        if (witnesses.size() == topK) {
            long raisedThreshold = Math.max(minUtil, witnesses.peek());
            if (raisedThreshold > minUtil) {
                minUtil = raisedThreshold;
                traceEvent("MIN_UTIL_UPDATE", "TRANSACTION_WITNESS", minUtil);
            }
        }
    }

    public static void insertionSort(List<Integer> items, long[] utilityBinArrayTWU) {
        for (int j = 1; j < items.size(); j++) {
            Integer itemJ = items.get(j);
            int i = j - 1;
            Integer itemI = items.get(i);
            long comparison = utilityBinArrayTWU[itemI] - utilityBinArrayTWU[itemJ];
            if (comparison == 0) comparison = itemI - itemJ;
            while (comparison > 0) {
                items.set(i + 1, itemI);
                i--;
                if (i < 0) break;
                itemI = items.get(i);
                comparison = utilityBinArrayTWU[itemI] - utilityBinArrayTWU[itemJ];
                if (comparison == 0) comparison = itemI - itemJ;
            }
            items.set(i + 1, itemJ);
        }
    }

    private void offerTopK(int[] items, long utility) {
        // minUtil is volatile/java-visible and never decreases. A candidate
        // strictly below it cannot enter the final top-k, so do not make every
        // worker acquire the global heap lock merely to reject that candidate.
        if (utility < minUtil) return;

        synchronized (topKQueue) {
            // The threshold may have risen while this worker was waiting.
            if (utility < minUtil) return;
            TopKPattern candidate = new TopKPattern(items, utility);
            if (topKQueue.size() < topK) {
                topKQueue.offer(candidate);
            } else if (TopKPattern.WORST_FIRST.compare(
                    candidate,
                    topKQueue.peek()
            ) > 0) {
                topKQueue.poll();
                topKQueue.offer(candidate);
            }

            if (topKQueue.size() == topK) {
                // A transaction certificate may already provide a greater safe
                // lower bound before the exact-result heap becomes full.
                long raisedThreshold = Math.max(
                        minUtil,
                        topKQueue.peek().utility
                );
                if (raisedThreshold > minUtil) {
                    minUtil = raisedThreshold;
                    traceEvent("MIN_UTIL_UPDATE", "TOP_K_HEAP", minUtil);
                }
            }
        }
    }

    private void traceEvent(String event, String source, long threshold) {
        ThresholdTraceLogger logger = thresholdTraceLogger;
        if (logger != null) {
            logger.record(
                    event,
                    source,
                    currentPartitionIndex,
                    currentPartitionItem,
                    threshold
            );
        }
    }

    /**
     * Writes trace events on a dedicated thread. Mining workers only timestamp
     * and enqueue an event; they never perform file I/O while holding top-k's
     * global heap monitor.
     */
    private static final class ThresholdTraceLogger {
        private static final TraceEvent STOP = new TraceEvent(
                -1L, -1L, "STOP", "STOP", -1, -1, -1L
        );
        private final long startTimestamp;
        private final AtomicLong sequence = new AtomicLong();
        private final LinkedBlockingQueue<TraceEvent> events =
                new LinkedBlockingQueue<>();
        private final AtomicReference<IOException> failure =
                new AtomicReference<>();
        private final Thread writerThread;
        private volatile boolean closed;

        ThresholdTraceLogger(File outputFile, long startTimestamp)
                throws IOException {
            this.startTimestamp = startTimestamp;
            File parent = outputFile.getParentFile();
            if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
                throw new IOException(
                        "Cannot create threshold trace directory: " + parent
                );
            }
            writerThread = new Thread(
                    () -> writeEvents(outputFile),
                    "threshold-trace-writer"
            );
            writerThread.setDaemon(true);
            writerThread.start();
        }

        void record(String event,
                    String source,
                    int partitionIndex,
                    int partitionItem,
                    long minUtil) {
            if (closed || failure.get() != null) return;
            long wallTime = System.currentTimeMillis();
            events.offer(new TraceEvent(
                    sequence.getAndIncrement(),
                    wallTime - startTimestamp,
                    event,
                    source,
                    partitionIndex,
                    partitionItem,
                    minUtil
            ));
        }

        private void writeEvents(File outputFile) {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outputFile, false),
                            StandardCharsets.UTF_8
                    ),
                    1 << 16
            )) {
                writer.write("sequence,event,source,elapsed_ms,wall_time_ms,"
                        + "partition_index,partition_item,min_util");
                writer.newLine();
                int eventsSinceFlush = 0;
                while (true) {
                    TraceEvent trace = events.take();
                    if (trace == STOP) break;
                    writer.write(trace.toCsv(startTimestamp));
                    writer.newLine();
                    eventsSinceFlush++;
                    if (eventsSinceFlush >= 256
                            || "PARTITION_START".equals(trace.event)) {
                        writer.flush();
                        eventsSinceFlush = 0;
                    }
                }
                writer.flush();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure.compareAndSet(
                        null,
                        new IOException("Threshold trace writer interrupted", interrupted)
                );
            } catch (IOException error) {
                failure.compareAndSet(null, error);
            }
        }

        synchronized void close() throws IOException, InterruptedException {
            if (!closed) {
                closed = true;
                events.offer(STOP);
                writerThread.join();
            }
            IOException error = failure.get();
            if (error != null) throw error;
        }
    }

    private static final class TraceEvent {
        final long sequence;
        final long elapsedMillis;
        final String event;
        final String source;
        final int partitionIndex;
        final int partitionItem;
        final long minUtil;

        TraceEvent(long sequence,
                   long elapsedMillis,
                   String event,
                   String source,
                   int partitionIndex,
                   int partitionItem,
                   long minUtil) {
            this.sequence = sequence;
            this.elapsedMillis = elapsedMillis;
            this.event = event;
            this.source = source;
            this.partitionIndex = partitionIndex;
            this.partitionItem = partitionItem;
            this.minUtil = minUtil;
        }

        String toCsv(long startTimestamp) {
            return sequence
                    + "," + event
                    + "," + source
                    + "," + elapsedMillis
                    + "," + (startTimestamp + elapsedMillis)
                    + "," + partitionIndex
                    + "," + partitionItem
                    + "," + minUtil;
        }
    }

    private Itemsets buildTopKResult() {
        List<TopKPattern> patterns = new ArrayList<>(topKQueue);
        patterns.sort(TopKPattern.BEST_FIRST);

        Itemsets result = new Itemsets("Top-" + topK + " high-utility itemsets");
        for (TopKPattern pattern : patterns) {
            int[] items = Arrays.copyOf(pattern.items, pattern.items.length);
            result.addItemset(new Itemset(items, pattern.utility), items.length);
        }
        return result;
    }

    public void printStats() {
        System.out.println("============= PTM-STYLE BASELINE STATS =============");
        System.out.println(" Top-k requested   : " + topK);
        System.out.println(" Results returned  : " + patternCount);
        System.out.println(" Initial minUtil-1 : " + thresholdAfterSingles);
        System.out.println(" Final minUtil     : " + minUtil);
        System.out.println(" Execution mode    : "
                + executionModeName());
        System.out.println(" Candidate pool    : " + activateCandidateParallelism);
        System.out.println(" Pool workers      : "
                + (activateCandidateParallelism ? candidateWorkerCount : 1));
        System.out.println(" Task admission    : " + candidateTaskAdmissionName());
        System.out.println(" Min task trans.   : "
                + minimumTransactionsPerCandidateTask);
        System.out.println(" Global pair U     : " + activateGlobalPairUtilityRaising);
        System.out.println(" Object-free proj. : " + activateObjectFreeProjection);
        System.out.println(" Memory-bound proj.: "
                + (activateMemoryBoundedProjection
                ? projectionBudgetMegabytes + " MB"
                : "off"));
        if (activateMemoryBoundedProjection
                && minimumEffectiveProjectionBudgetBytes != Long.MAX_VALUE) {
            System.out.println(" Effective MBP MB  : "
                    + String.format(
                    java.util.Locale.ROOT,
                    "%.1f..%.1f",
                    minimumEffectiveProjectionBudgetBytes / 1024D / 1024D,
                    maximumEffectiveProjectionBudgetBytes / 1024D / 1024D
            ));
            System.out.println(" Zero-MBP parts    : "
                    + zeroEffectiveProjectionBudgetPartitions);
        }
        System.out.println(" Work-aware SU     : " + activateWorkAwarePriority);
        System.out.println(" Partition priority: "
                + (activateWorkAwarePriority
                ? "SU/estimatedWork"
                : "AVG_TU"));
        System.out.println(" DFS partitions    : " + serialDfsPartitionCount);
        System.out.println(" Pool partitions   : " + candidatePoolPartitionCount);
        System.out.println(" Diagnostics       : " + diagnosticStatistics);
        if (diagnosticStatistics) {
            System.out.println(" Candidates        : "
                    + diagnosticCandidateCount.sum());
            System.out.println(" Async tasks       : "
                    + diagnosticAsyncTaskCount.sum());
            System.out.println(" Small DFS switches: "
                    + diagnosticSmallDfsSwitchCount.sum());
            System.out.println(" Queue inline      : "
                    + diagnosticQueueOverflowInlineCount.sum());
            System.out.println(" Lazy projections  : "
                    + diagnosticLazyProjectionCount.sum());
        }
        System.out.println(" Time ms           : " + (endTimestamp - startTimestamp));
        System.out.println(" Max memory MB     : " + maximumMemoryUsageMb);
        System.out.println(" Partition dir     : " + partitionDir.getAbsolutePath());
        System.out.println("====================================================");
    }

    private String executionModeName() {
        if (!activateCandidateParallelism) return "SERIAL_DFS";
        return activateWorkAwarePriority
                ? "WORK_AWARE_SU_POOL"
                : "FIFO_POOL";
    }

    private String candidateTaskAdmissionName() {
        return "FIXED_" + Math.max(
                candidateWorkerCount,
                maximumOutstandingCandidateTasks
        );
    }

    private static final class Phase1Stats {
        final int maxItem;
        final long[] twu;
        final long[] support;
        final long[] singletonUtility;
        long[][] globalPairUtility;
        TransactionCertificateStore transactionCertificates;
        int transactionCount;

        Phase1Stats(int maxItem,
                    int transactionCertificateLimit,
                    boolean allocateGlobalPairUtility) {
            this.maxItem = maxItem;
            this.twu = new long[maxItem + 1];
            this.support = new long[maxItem + 1];
            this.singletonUtility = new long[maxItem + 1];
            if (allocateGlobalPairUtility) {
                try {
                    this.globalPairUtility = new long[maxItem + 1][maxItem + 1];
                } catch (OutOfMemoryError error) {
                    printOOMRam("[GLOBAL PAIR MATRIX]", maxItem + 1);
                    throw error;
                }
            }
            if (transactionCertificateLimit > 0) {
                transactionCertificates =
                        new TransactionCertificateStore(transactionCertificateLimit);
            }
        }

        void offerTransactionCertificate(long fingerprint, long lowerBound) {
            if (transactionCertificates != null && lowerBound > 0L) {
                transactionCertificates.offer(fingerprint, lowerBound);
            }
        }

        void clearTransactionCertificates() {
            transactionCertificates = null;
        }

        void clearGlobalPairUtilities() {
            globalPairUtility = null;
        }
    }

    /** Fixed-memory top-k set for distinct transaction-itemset certificates. */
    private static final class TransactionCertificateStore {
        final int limit;
        final long[] heapKeys;
        final long[] heapValues;
        final long[] tableKeys;
        final int[] tableIndexes;
        final byte[] tableStates;
        final int tableMask;
        int size;
        int deletedCount;

        TransactionCertificateStore(int limit) {
            this.limit = limit;
            this.heapKeys = new long[limit];
            this.heapValues = new long[limit];

            int tableCapacity = 1;
            while (tableCapacity < limit * 2L) {
                tableCapacity <<= 1;
            }
            tableKeys = new long[tableCapacity];
            tableIndexes = new int[tableCapacity];
            tableStates = new byte[tableCapacity];
            tableMask = tableCapacity - 1;
        }

        void offer(long key, long lowerBound) {
            if (findExistingSlot(key) >= 0) return;

            if (size < limit) {
                int index = size++;
                heapKeys[index] = key;
                heapValues[index] = lowerBound;
                insertMapping(key, index);
                siftUp(index);
                return;
            }

            if (lowerBound <= heapValues[0]) return;

            removeMapping(heapKeys[0]);
            heapKeys[0] = key;
            heapValues[0] = lowerBound;
            insertMapping(key, 0);
            siftDown(0);
            if (deletedCount > tableStates.length / 4) {
                rebuildMappings();
            }
        }

        private void siftUp(int index) {
            while (index > 0) {
                int parent = (index - 1) >>> 1;
                if (heapValues[parent] <= heapValues[index]) return;
                swap(parent, index);
                index = parent;
            }
        }

        private void siftDown(int index) {
            while (true) {
                int left = (index << 1) + 1;
                if (left >= size) return;
                int right = left + 1;
                int smallest = right < size && heapValues[right] < heapValues[left]
                        ? right : left;
                if (heapValues[index] <= heapValues[smallest]) return;
                swap(index, smallest);
                index = smallest;
            }
        }

        private void swap(int left, int right) {
            long key = heapKeys[left];
            heapKeys[left] = heapKeys[right];
            heapKeys[right] = key;

            long value = heapValues[left];
            heapValues[left] = heapValues[right];
            heapValues[right] = value;

            updateMapping(heapKeys[left], left);
            updateMapping(heapKeys[right], right);
        }

        private int findExistingSlot(long key) {
            int slot = (int) mix64(key) & tableMask;
            for (int probes = 0; probes < tableStates.length; probes++) {
                if (tableStates[slot] == 0) return -1;
                if (tableStates[slot] == 1 && tableKeys[slot] == key) return slot;
                slot = (slot + 1) & tableMask;
            }
            return -1;
        }

        private void insertMapping(long key, int heapIndex) {
            int slot = (int) mix64(key) & tableMask;
            int firstDeleted = -1;
            for (int probes = 0; probes < tableStates.length; probes++) {
                if (tableStates[slot] == 0) break;
                if (tableStates[slot] == 2 && firstDeleted < 0) firstDeleted = slot;
                slot = (slot + 1) & tableMask;
            }
            if (firstDeleted >= 0) {
                slot = firstDeleted;
                deletedCount--;
            }
            tableStates[slot] = 1;
            tableKeys[slot] = key;
            tableIndexes[slot] = heapIndex;
        }

        private void removeMapping(long key) {
            int slot = findExistingSlot(key);
            if (slot >= 0) {
                tableStates[slot] = 2;
                deletedCount++;
            }
        }

        private void updateMapping(long key, int heapIndex) {
            int slot = findExistingSlot(key);
            if (slot < 0) throw new IllegalStateException("Missing certificate mapping");
            tableIndexes[slot] = heapIndex;
        }

        private void rebuildMappings() {
            Arrays.fill(tableStates, (byte) 0);
            deletedCount = 0;
            for (int index = 0; index < size; index++) {
                insertMapping(heapKeys[index], index);
            }
        }
    }

    private static final class RootPairWorkspace {
        final long[] exact;
        final long[] lu;
        final long[] su;
        final long[] suffixWork;
        final int[] marks;
        final int[] touched;
        int epoch;
        int touchedCount;

        RootPairWorkspace(int itemCount) {
            exact = new long[itemCount + 1];
            lu = new long[itemCount + 1];
            su = new long[itemCount + 1];
            suffixWork = new long[itemCount + 1];
            marks = new int[itemCount + 1];
            touched = new int[itemCount + 1];
        }

        void reset() {
            touchedCount = 0;
            epoch++;
            if (epoch == 0) {
                Arrays.fill(marks, 0);
                epoch = 1;
            }
        }

        void touch(int item) {
            if (marks[item] != epoch) {
                marks[item] = epoch;
                exact[item] = 0L;
                lu[item] = 0L;
                su[item] = 0L;
                suffixWork[item] = 0L;
                touched[touchedCount++] = item;
            }
        }

        long luValue(int item) {
            return marks[item] == epoch ? lu[item] : 0L;
        }

        long suValue(int item) {
            return marks[item] == epoch ? su[item] : 0L;
        }

        long estimatedWorkValue(int item, int parentTransactionCount) {
            long suffix = marks[item] == epoch ? suffixWork[item] : 0L;
            return parentTransactionCount + suffix;
        }
    }

    private static final class TopKPattern {
        static final Comparator<TopKPattern> WORST_FIRST = (left, right) -> {
            int byUtility = Long.compare(left.utility, right.utility);
            return byUtility != 0 ? byUtility : -compareItems(left.items, right.items);
        };

        static final Comparator<TopKPattern> BEST_FIRST = (left, right) -> {
            int byUtility = Long.compare(right.utility, left.utility);
            return byUtility != 0 ? byUtility : compareItems(left.items, right.items);
        };

        final int[] items;
        final long utility;

        TopKPattern(int[] items, long utility) {
            this.items = items;
            this.utility = utility;
        }

        private static int compareItems(int[] left, int[] right) {
            int common = Math.min(left.length, right.length);
            for (int i = 0; i < common; i++) {
                int comparison = Integer.compare(left[i], right[i]);
                if (comparison != 0) return comparison;
            }
            return Integer.compare(left.length, right.length);
        }
    }

    private static final class PartitionInfo {
        final File file;
        int transactionCount;
        int occurrenceCount;
        long sumLength;
        long sumTransactionUtility;
        long subtreeUtility;

        PartitionInfo(File file) {
            this.file = file;
        }
    }
}

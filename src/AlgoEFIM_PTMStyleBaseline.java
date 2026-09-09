/*
 * PTM-style baseline converted from EFIM-style code.
 * Purpose:
 *   - Build prefix-based suffix partitions by scanning the reduced DB once.
 *   - Load each partition fully into RAM.
 *   - Mine each partition using a candidate-level worker pool.
 *   - No adaptive RAM/Disk swap. If a partition/projected DB is too large, it may OOM.
 *
 * Input format: items : transactionUtility : utilities
 * Example: 1 2 3 : 30 : 5 10 15
 */


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public class AlgoEFIM_PTMStyleBaseline {

    private int topK;
    private volatile long minUtil;
    private volatile boolean topKThresholdCertified;
    private PriorityQueue<TopKPattern> topKQueue;
    private long thresholdAfterSingles;
    private long startTimestamp;
    private long endTimestamp;

    private long patternCount;
    private boolean activateTransactionMerging;
    private boolean activateSubtreeUtilityPruning;
    private boolean activateCandidateParallelism;
    private boolean activateDirectUtilityRaising;
    private boolean activateBestSUFirst;
    private boolean activateTransactionUtilityRaising;
    private boolean activateThresholdReadyParallelism;
    private boolean activateBestChildContinuation;
    private int candidateWorkerCount = Math.max(1, Runtime.getRuntime().availableProcessors());
    private int minimumTransactionsPerCandidateTask = 1;
    private boolean diagnosticStatistics;
    private final LongAdder diagnosticCandidateCount = new LongAdder();
    private final LongAdder diagnosticAsyncTaskCount = new LongAdder();
    private final LongAdder diagnosticSmallDfsSwitchCount = new LongAdder();
    private final LongAdder diagnosticQueueOverflowInlineCount = new LongAdder();
    /** Zero selects heap-pressure-aware admission; positive values select a fixed limit. */
    private int maximumOutstandingCandidateTasks = 1024;
    private static final double QUEUE_HEAP_PRESSURE_PAUSE_RATIO = 0.72d;
    private static final double QUEUE_HEAP_PRESSURE_RESUME_RATIO = 0.60d;
    private static final long QUEUE_HEAP_PROBE_MASK = 31L;
    private long serialDfsPartitionCount;
    private long candidatePoolPartitionCount;

    private long[] utilityBinArrayLU;
    private long[] utilityBinArraySU;

    private int[] oldNameToNewNames;
    private int[] newNamesToOldNames;
    private int newItemCount;
    private static final int MAXIMUM_SIZE_MERGING = 1000;
    private static final int IO_BUFFER = 1 << 13;

    private File partitionDir;

    public void configureCandidateParallelism(boolean enabled,
                                              int workerCount,
                                              boolean directUtilityRaising,
                                              boolean bestSUFirst) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be greater than 0");
        }
        this.activateCandidateParallelism = enabled;
        this.candidateWorkerCount = workerCount;
        this.activateDirectUtilityRaising = directUtilityRaising;
        this.activateBestSUFirst = bestSUFirst;
    }

    public void configureTransactionUtilityRaising(boolean enabled) {
        this.activateTransactionUtilityRaising = enabled;
    }

    /**
     * Delay the candidate pool until k valid pattern witnesses certify a
     * top-k lower bound. Witnesses may come from the exact result heap or from
     * a safe threshold-raising strategy. The warm-up uses serial DFS.
     */
    public void configureThresholdReadyParallelism(boolean enabled,
                                                   boolean bestChildContinuation) {
        this.activateThresholdReadyParallelism = enabled;
        this.activateBestChildContinuation = bestChildContinuation;
    }

    public void configureCandidateTaskLimit(int maximumOutstandingTasks) {
        if (maximumOutstandingTasks < 0) {
            throw new IllegalArgumentException(
                    "maximumOutstandingTasks must be 0 (adaptive) or greater");
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

    /** Diagnostic counters add hot-path overhead; keep disabled for timed runs. */
    public void configureDiagnosticStatistics(boolean enabled) {
        this.diagnosticStatistics = enabled;
    }

    public Itemsets runAlgorithm(int requestedK,
                                 String inputPath,
                                 String outputPath,
                                 boolean activateTransactionMerging,
                                 int maximumTransactionCount,
                                 boolean activateSubtreeUtilityPruning) throws IOException, InterruptedException {

        if (requestedK <= 0) {
            throw new IllegalArgumentException("k must be greater than 0");
        }
        this.topK = requestedK;
        // TKEH Algorithm 4 starts at 1 for positive-utility databases.
        // This also prevents nonexistent zero-utility itemsets from entering
        // the result when k is larger than the number of observed patterns.
        this.minUtil = 1L;
        this.topKThresholdCertified = false;
        this.topKQueue = new PriorityQueue<>(requestedK, TopKPattern.WORST_FIRST);
        this.thresholdAfterSingles = 0L;
        this.activateTransactionMerging = activateTransactionMerging;
        this.activateSubtreeUtilityPruning = activateSubtreeUtilityPruning;
        patternCount = 0;
        diagnosticCandidateCount.reset();
        diagnosticAsyncTaskCount.reset();
        diagnosticSmallDfsSwitchCount.reset();
        diagnosticQueueOverflowInlineCount.reset();
        serialDfsPartitionCount = 0L;
        candidatePoolPartitionCount = 0L;
        startTimestamp = System.currentTimeMillis();

        // ==========================
        // PHASE 1: TWU + max item
        // ==========================
        Phase1Stats stats = phase1ScanStats(inputPath, maximumTransactionCount);
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

        // Reused while each partition is loaded. No global pair-matrix and no
        // extra pass over all partition files are required.
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

        // PTM partition-selection strategy: partitions with the greatest
        // average suffix-transaction utility are processed first so that the
        // border threshold is likely to rise earlier.
        itemsToExplore.sort((left, right) -> {
            int byAverage = Double.compare(
                    averageTransactionUtility(partitions[right]),
                    averageTransactionUtility(partitions[left])
            );
            return byAverage != 0 ? byAverage : Integer.compare(left, right);
        });


        // ==========================
        // PHASE 3: process each prefix partition fully in RAM
        // ==========================
        System.out.println("[PTM-STYLE] Mining partitions in RAM...");
        System.out.println("[MINING] mode="
                + executionModeName()
                + " | workers=" + (activateCandidateParallelism ? candidateWorkerCount : 1)
                + " | taskAdmission=" + candidateTaskAdmissionName()
                + " | minTaskTransactions=" + minimumTransactionsPerCandidateTask
                + " | directUtilityRaising=" + activateDirectUtilityRaising
                + " | bestSUFirst=" + activateBestSUFirst
                + " | thresholdReady=" + activateThresholdReadyParallelism
                + " | bestChildContinuation=" + activateBestChildContinuation);
        // The disabled mode is a genuine recursive depth-first traversal.
        // Candidate tasks and the shared priority pool exist only in parallel mode.
        CandidateScheduler candidateScheduler = activateCandidateParallelism
                ? new CandidateScheduler(candidateWorkerCount)
                : null;
        try {
            for (int i = 0; i < itemsToExplore.size(); i++) {
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

                List<Transaction> partitionTransactions;
                partitionTransactions = loadPartitionIntoRam(p.file, p.transactionCount);

                calculateRootPairBounds(partitionTransactions, rootPairWorkspace);
                offerRootPairUtilities(e, rootPairWorkspace);
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
                    List<Integer> rootItemsToExplore = activateSubtreeUtilityPruning
                            ? newItemsToExplore
                            : newItemsToKeep;
                    boolean useCandidatePool = activateCandidateParallelism
                            && (!activateThresholdReadyParallelism
                            || topKThresholdCertified);
                    if (useCandidatePool) {
                        if (candidatePoolPartitionCount == 0L
                                && activateThresholdReadyParallelism) {
                            System.out.println("[THRESHOLD-READY] exactHeap="
                                    + topKQueue.size()
                                    + " | minUtil=" + minUtil
                                    + " | enabling candidate pool");
                        }
                        candidatePoolPartitionCount++;
                        candidateScheduler.minePartition(
                                e,
                                partitionTransactions,
                                newItemsToKeep,
                                rootItemsToExplore,
                                rootPairWorkspace.su
                        );
                    } else {
                        serialDfsPartitionCount++;
                        minePartitionDepthFirst(
                                e,
                                partitionTransactions,
                                newItemsToKeep,
                                rootItemsToExplore,
                                rootPairWorkspace.su
                        );
                    }
                }

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
        //printStats();
        patternCount = topKQueue.size();
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

        int count = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file), IO_BUFFER))) {

            while (true) {
                Transaction t = readTransactionOrNull(dis);
                if (t == null) break;

                list.add(t);
                count++;

            }
        } catch (OutOfMemoryError oom) {
            System.err.println(
                    "[OOM WHILE LOAD] file=" + file.getName() +
                            " expectedTrans=" + expectedTrans +
                            " loaded=" + count +
                            " fileMB=" + file.length() / 1024 / 1024
            );
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
                                         List<Transaction> rootTransactions,
                                         List<Integer> itemsToKeep,
                                         List<Integer> itemsToExplore,
                                         long[] rootSU) {
        int[] keep = toIntArray(itemsToKeep);
        int[] extensions = toIntArray(itemsToExplore);
        long[] subtreeUtilities = new long[extensions.length];
        for (int index = 0; index < extensions.length; index++) {
            subtreeUtilities[index] = rootSU[extensions[index]];
        }
        sortCandidatesByDescendingSU(extensions, subtreeUtilities);

        int[] rootPrefix = new int[]{newNamesToOldNames[rootItem]};
        WorkerBins bins = new WorkerBins(newItemCount, activateDirectUtilityRaising);
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

    private void mineCandidateDepthFirst(List<Transaction> parent,
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

        if (activateDirectUtilityRaising) {
            for (int index = positionInKeep + 1; index < itemsToKeep.length; index++) {
                int child = itemsToKeep[index];
                long utility = bins.exactValue(child);
                if (utility >= minUtil) {
                    offerTopK(
                            appendItem(currentPrefix, newNamesToOldNames[child]),
                            utility
                    );
                }
            }
        }

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
        sortCandidatesByDescendingSU(children, childSubtreeUtilities);

        for (int index = 0; index < children.length; index++) {
            mineCandidateDepthFirst(
                    projection.transactions,
                    newItemsToKeep,
                    currentPrefix,
                    children[index],
                    childSubtreeUtilities[index],
                    activateDirectUtilityRaising,
                    bins
            );
        }
        projection.transactions.clear();
    }

    /** Keep sibling order unchanged unless the Best-SU-first flag is enabled. */
    private void sortCandidatesByDescendingSU(int[] candidates, long[] subtreeUtilities) {
        if (!activateBestSUFirst) return;
        for (int index = 1; index < candidates.length; index++) {
            int candidate = candidates[index];
            long utility = subtreeUtilities[index];
            int position = index - 1;
            while (position >= 0 && subtreeUtilities[position] < utility) {
                candidates[position + 1] = candidates[position];
                subtreeUtilities[position + 1] = subtreeUtilities[position];
                position--;
            }
            candidates[position + 1] = candidate;
            subtreeUtilities[position + 1] = utility;
        }
    }

    /**
     * A reusable worker pool where each task processes exactly one candidate.
     * Children are returned to the shared queue instead of being explored
     * recursively by the same worker.
     */
    private final class CandidateScheduler {
        private final PriorityBlockingQueue<CandidateTask> candidateQueue =
                new PriorityBlockingQueue<>();
        private final Semaphore queueSlots;
        private final AtomicInteger queuedTaskCount = new AtomicInteger();
        private final AtomicLong heapProbeSequence = new AtomicLong();
        private final Thread[] workers;
        private final AtomicLong sequence = new AtomicLong();
        private volatile boolean stopped;
        private volatile boolean heapPressureHigh;
        private final ThreadLocal<WorkerBins> workerBins =
                ThreadLocal.withInitial(() -> {
                    return new WorkerBins(
                            newItemCount,
                            activateDirectUtilityRaising
                    );
                });

        CandidateScheduler(int workerCount) {
            queueSlots = maximumOutstandingCandidateTasks == 0
                    ? null
                    : new Semaphore(Math.max(
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
                           List<Transaction> rootTransactions,
                           List<Integer> itemsToKeep,
                           List<Integer> itemsToExplore,
                           long[] rootSU) throws IOException, InterruptedException {
            PartitionRun run = new PartitionRun();
            int[] keep = toIntArray(itemsToKeep);
            int[] rootPrefix = new int[]{newNamesToOldNames[rootItem]};

            int[] extensions = toIntArray(itemsToExplore);
            long[] subtreeUtilities = new long[extensions.length];
            for (int index = 0; index < extensions.length; index++) {
                subtreeUtilities[index] = rootSU[extensions[index]];
            }
            sortCandidatesByDescendingSU(extensions, subtreeUtilities);

            // Producer guard: prevents a very fast initial task from making
            // pending reach zero while the remaining initial tasks are added.
            run.pending.incrementAndGet();
            for (int index = 0; index < extensions.length; index++) {
                enqueueCandidate(
                        run,
                        rootTransactions,
                        keep,
                        rootPrefix,
                        extensions[index],
                        subtreeUtilities[index],
                        true
                );
            }
            run.finishOne();
            run.completed.await();

            Throwable failure = run.failure.get();
            if (failure instanceof IOException) throw (IOException) failure;
            if (failure instanceof InterruptedException) throw (InterruptedException) failure;
            if (failure instanceof RuntimeException) throw (RuntimeException) failure;
            if (failure instanceof Error) throw (Error) failure;
            if (failure != null) throw new IOException("Candidate worker failed", failure);
        }

        private void enqueueCandidate(PartitionRun run,
                                      List<Transaction> parent,
                                      int[] itemsToKeep,
                                      int[] parentPrefix,
                                      int extension,
                                      long subtreeUtility,
                                      boolean utilityAlreadyOffered) {
            if (activateSubtreeUtilityPruning && subtreeUtility < minUtil) return;

            if (parent.size() < minimumTransactionsPerCandidateTask) {
                // This candidate is too small to amortize allocation, shared
                // priority-queue traffic, and worker hand-off. Descendant
                // projections cannot contain more transactions than this
                // parent, so switch the whole subtree to genuine serial DFS
                // instead of repeating scheduler checks at every child.
                if (diagnosticStatistics) {
                    diagnosticSmallDfsSwitchCount.increment();
                }
                mineCandidateDepthFirst(
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

            if (!tryAcquireQueueAdmission()) {
                // The fixed frontier is full or heap pressure is high. Continue
                // synchronously without allocating a CandidateTask or touching
                // the pending counter.
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
                    utilityAlreadyOffered,
                    sequence.getAndIncrement()
            );
            run.pending.incrementAndGet();
            if (diagnosticStatistics) diagnosticAsyncTaskCount.increment();
            candidateQueue.offer(task);
        }

        private boolean tryAcquireQueueAdmission() {
            if (queueSlots != null) {
                return queueSlots.tryAcquire();
            }

            int queued = queuedTaskCount.incrementAndGet();

            // Always retain enough shared work to feed every worker. Beyond
            // that floor, stop growing the frontier when the live Java heap is
            // under pressure. This is admission control, not instrumentation:
            // candidates rejected here execute synchronously and are not lost.
            if (queued <= workers.length) return true;

            if ((heapProbeSequence.getAndIncrement() & QUEUE_HEAP_PROBE_MASK) == 0L) {
                Runtime runtime = Runtime.getRuntime();
                long usedHeap = runtime.totalMemory() - runtime.freeMemory();
                double usageRatio = (double) usedHeap / (double) runtime.maxMemory();
                if (heapPressureHigh) {
                    if (usageRatio <= QUEUE_HEAP_PRESSURE_RESUME_RATIO) {
                        heapPressureHigh = false;
                    }
                } else if (usageRatio >= QUEUE_HEAP_PRESSURE_PAUSE_RATIO) {
                    heapPressureHigh = true;
                }
            }

            if (!heapPressureHigh) return true;
            queuedTaskCount.decrementAndGet();
            return false;
        }

        private void releaseQueueAdmission() {
            if (queueSlots != null) {
                queueSlots.release();
            } else {
                queuedTaskCount.decrementAndGet();
            }
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
            final PartitionRun run;
            final List<Transaction> parent;
            final int[] itemsToKeep;
            final int[] parentPrefix;
            final int extension;
            final long subtreeUtility;
            final boolean utilityAlreadyOffered;
            final long sequenceNumber;

            CandidateTask(PartitionRun run,
                          List<Transaction> parent,
                          int[] itemsToKeep,
                          int[] parentPrefix,
                          int extension,
                          long subtreeUtility,
                          boolean utilityAlreadyOffered,
                          long sequenceNumber) {
                this.run = run;
                this.parent = parent;
                this.itemsToKeep = itemsToKeep;
                this.parentPrefix = parentPrefix;
                this.extension = extension;
                this.subtreeUtility = subtreeUtility;
                this.utilityAlreadyOffered = utilityAlreadyOffered;
                this.sequenceNumber = sequenceNumber;
            }

            @Override
            public int compareTo(CandidateTask other) {
                if (activateBestSUFirst) {
                    int bySU = Long.compare(other.subtreeUtility, subtreeUtility);
                    if (bySU != 0) return bySU;
                }
                return Long.compare(sequenceNumber, other.sequenceNumber);
            }

            @Override
            public void run() {
                try {
                    if (run.failure.get() != null) return;
                    if (activateSubtreeUtilityPruning && subtreeUtility < minUtil) return;
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
                } catch (Throwable failure) {
                    run.fail(failure);
                } finally {
                    run.finishOne();
                }
            }
        }

        private void processCandidate(PartitionRun run,
                                      List<Transaction> parent,
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

            if (projection.transactions.isEmpty()) return;

            // Exact utilities of all immediate children are available from
            // the same suffix loop that calculates LU and SU.
            if (activateDirectUtilityRaising) {
                for (int index = positionInKeep + 1;
                     index < itemsToKeep.length; index++) {
                    int child = itemsToKeep[index];
                    long utility = bins.exactValue(child);
                    if (utility >= minUtil) {
                        offerTopK(
                                appendItem(currentPrefix, newNamesToOldNames[child]),
                                utility
                        );
                    }
                }
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

            // A continuation or queue-overflow child reuses and resets this
            // thread's WorkerBins.
            // Snapshot every priority/bound before starting any child, or the
            // parent would read zero/stale SU values for later siblings.
            long[] childSubtreeUtilities = new long[children.length];
            for (int index = 0; index < children.length; index++) {
                childSubtreeUtilities[index] = bins.suValue(children[index]);
            }

            int continuationIndex = -1;
            if (activateBestChildContinuation && children.length > 0) {
                continuationIndex = 0;
                for (int index = 1; index < children.length; index++) {
                    if (childSubtreeUtilities[index]
                            > childSubtreeUtilities[continuationIndex]) {
                        continuationIndex = index;
                    }
                }

                // Work-first threshold leader: do not make the strongest child
                // wait behind the shared frontier. Its siblings remain pool
                // candidates after this continuation returns.
                processCandidate(
                        run,
                        projection.transactions,
                        newItemsToKeep,
                        currentPrefix,
                        children[continuationIndex],
                        childSubtreeUtilities[continuationIndex],
                        activateDirectUtilityRaising,
                        bins
                );
                if (run.failure.get() != null) return;
            }

            for (int index = 0; index < children.length; index++) {
                if (index == continuationIndex) continue;
                if (activateSubtreeUtilityPruning
                        && childSubtreeUtilities[index] < minUtil) {
                    continue;
                }
                int child = children[index];
                enqueueCandidate(
                        run,
                        projection.transactions,
                        newItemsToKeep,
                        currentPrefix,
                        child,
                        childSubtreeUtilities[index],
                        activateDirectUtilityRaising
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
        final long[] exact;
        final int[] marks;
        final int[] touched;
        int epoch;
        int touchedCount;

        WorkerBins(int itemCount, boolean trackExactUtility) {
            lu = new long[itemCount + 1];
            su = new long[itemCount + 1];
            exact = trackExactUtility ? new long[itemCount + 1] : null;
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
                if (exact != null) exact[item] = 0L;
                touched[touchedCount++] = item;
            }
        }


        long luValue(int item) {
            return marks[item] == epoch ? lu[item] : 0L;
        }

        long suValue(int item) {
            return marks[item] == epoch ? su[item] : 0L;
        }

        long exactValue(int item) {
            return exact != null && marks[item] == epoch ? exact[item] : 0L;
        }
    }

    private static final class ParallelBuildResult {
        final List<Transaction> transactions;
        final long utility;

        ParallelBuildResult(List<Transaction> transactions,
                            long utility) {
            this.transactions = transactions;
            this.utility = utility;
        }
    }

    private ParallelBuildResult buildProjectedNodeReadOnly(List<Transaction> parent,
                                                           int extension,
                                                           WorkerBins bins) {
        List<Transaction> projectedTransactions = new ArrayList<>(parent.size());
        long utility = 0L;
        Transaction previous = null;
        int consecutiveMergeCount = 0;

        for (Transaction transaction : parent) {
            int position = Arrays.binarySearch(
                    transaction.items,
                    transaction.offset,
                    transaction.items.length,
                    extension
            );
            if (position < 0) continue;

            if (position == transaction.getLastPosition()) {
                utility += transaction.prefixUtility + transaction.utilities[position];
                continue;
            }

            Transaction projected = new Transaction(transaction, position);
            utility += projected.prefixUtility;

            long remainingUtility = 0L;
            for (int index = projected.items.length - 1;
                 index >= projected.offset; index--) {
                int item = projected.items[index];
                bins.touch(item);
                remainingUtility += projected.utilities[index];
                bins.su[item] += projected.prefixUtility + remainingUtility;
                bins.lu[item] += projected.prefixUtility + projected.transactionUtility;
                if (bins.exact != null) {
                    bins.exact[item] +=
                            projected.prefixUtility + projected.utilities[index];
                }
            }

            if (activateTransactionMerging
                    && MAXIMUM_SIZE_MERGING >= transaction.items.length - position) {
                if (previous == null) {
                    previous = projected;
                } else if (isEqualTo(projected, previous)) {
                    previous = mergeProjectedTransactions(
                            previous,
                            projected,
                            consecutiveMergeCount
                    );
                    consecutiveMergeCount++;
                } else {
                    projectedTransactions.add(previous);
                    previous = projected;
                    consecutiveMergeCount = 0;
                }
            } else {
                projectedTransactions.add(projected);
            }
        }

        if (previous != null) {
            projectedTransactions.add(previous);
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
    }

    private void deleteDirectory(File dir) throws IOException {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDirectory(f);
                else if (!f.delete()) throw new IOException("Cannot delete file: " + f.getAbsolutePath());
            }
        }
        if (!dir.delete()) throw new IOException("Cannot delete dir: " + dir.getAbsolutePath());
    }

    private Phase1Stats phase1ScanStats(String inputPath, int maximumTransactionCount) throws IOException {
        int maxItem = 0;
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
                activateTransactionUtilityRaising ? topK : 0
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
                if (activateTransactionUtilityRaising && items.length >= 2) {
                    stats.offerTransactionCertificate(
                            transactionFingerprint(items),
                            tu
                    );
                }
                for (int i = 0; i < items.length; i++) {
                    int item = Integer.parseInt(items[i]);
                    stats.twu[item] += tu;
                    stats.support[item]++;
                    stats.singletonUtility[item] += Long.parseLong(utilities[i]);
                }
                if (count == maximumTransactionCount) break;
            }
        }
        return stats;
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
            minUtil = Math.max(minUtil, witnesses.peek());
            topKThresholdCertified = true;
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

    private boolean isEqualTo(Transaction t1, Transaction t2) {
        int length1 = t1.items.length - t1.offset;
        int length2 = t2.items.length - t2.offset;
        if (length1 != length2) return false;
        int p1 = t1.offset;
        int p2 = t2.offset;
        while (p1 < t1.items.length) {
            if (t1.items[p1] != t2.items[p2]) return false;
            p1++;
            p2++;
        }
        return true;
    }

    private Transaction mergeProjectedTransactions(Transaction previous, Transaction projected, int consecutiveMergeCount) {
        if (consecutiveMergeCount == 0) {
            int itemsCount = previous.items.length - previous.offset;
            int[] items = new int[itemsCount];
            long[] utils = new long[itemsCount];
            System.arraycopy(previous.items, previous.offset, items, 0, itemsCount);
            System.arraycopy(previous.utilities, previous.offset, utils, 0, itemsCount);

            int pp = 0;
            int pj = projected.offset;
            while (pp < itemsCount) {
                utils[pp] += projected.utilities[pj];
                pp++;
                pj++;
            }

            Transaction merged = new Transaction(items, utils, previous.transactionUtility + projected.transactionUtility);
            merged.prefixUtility = previous.prefixUtility + projected.prefixUtility;
            return merged;
        } else {
            int pp = 0;
            int pj = projected.offset;
            while (pp < previous.items.length) {
                previous.utilities[pp] += projected.utilities[pj];
                pp++;
                pj++;
            }
            previous.transactionUtility += projected.transactionUtility;
            previous.prefixUtility += projected.prefixUtility;
            return previous;
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
            } else if (TopKPattern.WORST_FIRST.compare(candidate, topKQueue.peek()) > 0) {
                topKQueue.poll();
                topKQueue.offer(candidate);
            }

            if (topKQueue.size() == topK) {
                // A transaction certificate may already provide a greater safe
                // lower bound before the exact-result heap becomes full.
                minUtil = Math.max(minUtil, topKQueue.peek().utility);
                topKThresholdCertified = true;
            }
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
        System.out.println(" Direct child U    : " + activateDirectUtilityRaising);
        System.out.println(" Best-SU-first     : " + activateBestSUFirst);
        System.out.println(" Threshold-ready   : " + activateThresholdReadyParallelism);
        System.out.println(" Threshold certified: " + topKThresholdCertified);
        System.out.println(" Best-child cont.  : " + activateBestChildContinuation);
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
        }
        System.out.println(" Time ms           : " + (endTimestamp - startTimestamp));
        System.out.println(" Partition dir     : " + partitionDir.getAbsolutePath());
        System.out.println("====================================================");
    }

    private String executionModeName() {
        if (!activateCandidateParallelism) return "SERIAL_DFS";
        return activateThresholdReadyParallelism
                ? "THRESHOLD_READY_DFS_POOL"
                : "CANDIDATE_POOL";
    }

    private String candidateTaskAdmissionName() {
        if (maximumOutstandingCandidateTasks > 0) {
            return "FIXED_" + Math.max(
                    candidateWorkerCount,
                    maximumOutstandingCandidateTasks
            );
        }
        return "ADAPTIVE_HEAP_72_60_PERCENT";
    }

    private static final class Phase1Stats {
        final int maxItem;
        final long[] twu;
        final long[] support;
        final long[] singletonUtility;
        TransactionCertificateStore transactionCertificates;
        int transactionCount;

        Phase1Stats(int maxItem, int transactionCertificateLimit) {
            this.maxItem = maxItem;
            this.twu = new long[maxItem + 1];
            this.support = new long[maxItem + 1];
            this.singletonUtility = new long[maxItem + 1];
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
        final int[] marks;
        final int[] touched;
        int epoch;
        int touchedCount;

        RootPairWorkspace(int itemCount) {
            exact = new long[itemCount + 1];
            lu = new long[itemCount + 1];
            su = new long[itemCount + 1];
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
                touched[touchedCount++] = item;
            }
        }

        long luValue(int item) {
            return marks[item] == epoch ? lu[item] : 0L;
        }

        long suValue(int item) {
            return marks[item] == epoch ? su[item] : 0L;
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

        PartitionInfo(File file) {
            this.file = file;
        }
    }
}

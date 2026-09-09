import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Independent, single-threaded correctness oracle based on TKEH Algorithms 4-5.
 *
 * It uses RIU and CUD threshold raising, sparse EUCS pair pruning,
 * increasing-TWU item order, projected databases, and utility arrays for
 * local/subtree upper bounds. COV is omitted because it is not required for
 * exactness and can create many extra combinations.
 *
 * Input utilities must be non-negative, as assumed by TKEH.
 */
public final class AlgoTKEHSerialVerifier {
    private int topK;
    private long minUtil;
    private long candidateCount;
    private long transactionReadCount;
    private long startTimestamp;
    private long endTimestamp;
    private int itemCount;
    private int[] newToOld;
    private PriorityQueue<Pattern> queue;
    private boolean activatePairRaising = true;
    private PairTable pairTable;

    public void configurePairRaising(boolean enabled) {
        activatePairRaising = enabled;
    }

    public Itemsets runAlgorithm(int requestedK,
                                 String inputPath,
                                 int maximumTransactionCount) throws IOException {
        if (requestedK <= 0) {
            throw new IllegalArgumentException("k must be greater than 0");
        }

        topK = requestedK;
        // Algorithm 4 of TKEH initializes the internal threshold to 1.
        minUtil = 1L;
        candidateCount = 0L;
        transactionReadCount = 0L;
        queue = new PriorityQueue<>(requestedK, Pattern.WORST_FIRST);
        pairTable = activatePairRaising ? new PairTable() : null;
        startTimestamp = System.currentTimeMillis();

        FirstScan stats = firstScan(inputPath, maximumTransactionCount);
        System.out.println("[SERIAL-TKEH] First scan completed"
                + " | distinctItems=" + stats.twu.size());

        // TKEH RIU strategy: exact utility of every singleton.
        for (Map.Entry<Integer, Long> entry : stats.riu.entrySet()) {
            offer(new int[]{entry.getKey()}, entry.getValue());
        }
        System.out.println("[SERIAL-TKEH] RIU completed"
                + " | threshold=" + minUtil);

        List<Integer> secondaryOldItems = new ArrayList<>();
        for (Integer item : stats.twu.keySet()) {
            if (stats.twu.get(item) >= minUtil) {
                secondaryOldItems.add(item);
            }
        }
        secondaryOldItems.sort((left, right) -> {
            int byTwu = Long.compare(stats.twu.get(left), stats.twu.get(right));
            return byTwu != 0 ? byTwu : Integer.compare(left, right);
        });

        itemCount = secondaryOldItems.size();
        newToOld = new int[itemCount + 1];
        Map<Integer, Integer> oldToNew = new HashMap<>(Math.max(16, itemCount * 2));
        for (int index = 0; index < itemCount; index++) {
            int newItem = index + 1;
            int oldItem = secondaryOldItems.get(index);
            oldToNew.put(oldItem, newItem);
            newToOld[newItem] = oldItem;
        }

        List<SerialTransaction> database = secondScan(
                inputPath,
                maximumTransactionCount,
                oldToNew
        );
        System.out.println("[SERIAL-TKEH] Reduced database loaded"
                + " | transactions=" + database.size()
                + " | secondaryItems=" + itemCount);

        if (pairTable != null) {
            raiseThresholdFromPairs();
            System.out.println("[SERIAL-TKEH] CUD pair raising completed"
                    + " | observedPairs=" + pairTable.size
                    + " | threshold=" + minUtil);
            pairTable.discardExactUtilities();
        }

        int[] secondary = new int[itemCount];
        for (int index = 0; index < itemCount; index++) {
            secondary[index] = index + 1;
        }

        Bounds rootBounds = calculateBounds(database);
        int[] primary = selectPrimary(secondary, rootBounds.su, minUtil);
        System.out.println("[SERIAL-TKEH] Mining"
                + " | primaryItems=" + primary.length
                + " | threshold=" + minUtil);
        search(new int[0], database, primary, secondary);

        endTimestamp = System.currentTimeMillis();
        return buildResult();
    }

    private void search(int[] prefix,
                        List<SerialTransaction> database,
                        int[] primary,
                        int[] secondary) {
        candidateCount += primary.length;

        for (int extension : primary) {
            int extensionIndex = Arrays.binarySearch(secondary, extension);
            if (extensionIndex < 0) {
                continue;
            }

            Projection projection = project(database, extension);
            int[] beta = append(prefix, newToOld[extension]);
            // RIU seeded singletons; when enabled, CUD also seeded all pairs.
            int seededLength = pairTable == null ? 1 : 2;
            if (beta.length > seededLength) {
                offer(beta, projection.utility);
            }

            if (projection.transactions.isEmpty()) {
                continue;
            }

            long threshold = minUtil;
            int suffixLength = secondary.length - extensionIndex - 1;
            int[] nextSecondaryBuffer = new int[suffixLength];
            int[] nextPrimaryBuffer = new int[suffixLength];
            int secondaryCount = 0;
            int primaryCount = 0;

            for (int index = extensionIndex + 1; index < secondary.length; index++) {
                int item = secondary[index];
                if (pairTable != null
                        && pairTable.getTwu(extension, item) < threshold) {
                    continue;
                }
                if (projection.bounds.lu[item] >= threshold) {
                    nextSecondaryBuffer[secondaryCount++] = item;
                    if (projection.bounds.su[item] >= threshold) {
                        nextPrimaryBuffer[primaryCount++] = item;
                    }
                }
            }

            if (primaryCount > 0) {
                search(
                        beta,
                        projection.transactions,
                        Arrays.copyOf(nextPrimaryBuffer, primaryCount),
                        Arrays.copyOf(nextSecondaryBuffer, secondaryCount)
                );
            }
        }
    }

    private Projection project(List<SerialTransaction> parent, int extension) {
        List<SerialTransaction> projected = new ArrayList<>(parent.size());
        long utility = 0L;

        for (SerialTransaction transaction : parent) {
            transactionReadCount++;
            int position = Arrays.binarySearch(
                    transaction.items,
                    transaction.offset,
                    transaction.items.length,
                    extension
            );
            if (position < 0) {
                continue;
            }

            long prefixUtility =
                    transaction.prefixUtility + transaction.utilities[position];
            utility += prefixUtility;

            if (position + 1 < transaction.items.length) {
                long remainingUtility = 0L;
                for (int index = position + 1; index < transaction.utilities.length; index++) {
                    remainingUtility += transaction.utilities[index];
                }
                projected.add(new SerialTransaction(
                        transaction.items,
                        transaction.utilities,
                        position + 1,
                        prefixUtility,
                        remainingUtility
                ));
            }
        }

        return new Projection(projected, utility, calculateBounds(projected));
    }

    /** Utility-array calculation for local utility (LU) and subtree utility (SU). */
    private Bounds calculateBounds(List<SerialTransaction> database) {
        Bounds bounds = new Bounds(itemCount);
        for (SerialTransaction transaction : database) {
            long suffixUtility = 0L;
            for (int index = transaction.items.length - 1;
                 index >= transaction.offset; index--) {
                int item = transaction.items[index];
                suffixUtility += transaction.utilities[index];
                bounds.su[item] += transaction.prefixUtility + suffixUtility;
                bounds.lu[item] +=
                        transaction.prefixUtility + transaction.remainingUtility;
            }
        }
        return bounds;
    }

    private int[] selectPrimary(int[] secondary, long[] su, long threshold) {
        int[] buffer = new int[secondary.length];
        int count = 0;
        for (int item : secondary) {
            if (su[item] >= threshold) {
                buffer[count++] = item;
            }
        }
        return Arrays.copyOf(buffer, count);
    }

    private FirstScan firstScan(String inputPath,
                                int maximumTransactionCount) throws IOException {
        FirstScan stats = new FirstScan();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                if (isMetadata(line)) {
                    continue;
                }
                ParsedLine parsed = parse(line);
                for (int index = 0; index < parsed.items.length; index++) {
                    int item = parsed.items[index];
                    stats.twu.merge(item, parsed.transactionUtility, Long::sum);
                    stats.riu.merge(item, parsed.utilities[index], Long::sum);
                }
                count++;
                if (count == maximumTransactionCount) {
                    break;
                }
            }
        }
        return stats;
    }

    private List<SerialTransaction> secondScan(String inputPath,
                                               int maximumTransactionCount,
                                               Map<Integer, Integer> oldToNew)
            throws IOException {
        List<SerialTransaction> database = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                if (isMetadata(line)) {
                    continue;
                }
                ParsedLine parsed = parse(line);
                int kept = 0;
                for (int item : parsed.items) {
                    if (oldToNew.containsKey(item)) {
                        kept++;
                    }
                }

                if (kept > 0) {
                    int[] items = new int[kept];
                    long[] utilities = new long[kept];
                    long remainingUtility = 0L;
                    int destination = 0;
                    for (int index = 0; index < parsed.items.length; index++) {
                        Integer renamed = oldToNew.get(parsed.items[index]);
                        if (renamed != null) {
                            items[destination] = renamed;
                            utilities[destination] = parsed.utilities[index];
                            remainingUtility += parsed.utilities[index];
                            destination++;
                        }
                    }
                    Transaction.insertionSort(items, utilities);

                    if (pairTable != null) {
                        for (int left = 0; left < items.length; left++) {
                            for (int right = left + 1; right < items.length; right++) {
                                pairTable.add(
                                        items[left],
                                        items[right],
                                        utilities[left] + utilities[right],
                                        parsed.transactionUtility
                                );
                            }
                        }
                    }

                    database.add(new SerialTransaction(
                            items,
                            utilities,
                            0,
                            0L,
                            remainingUtility
                    ));
                }

                count++;
                if (count == maximumTransactionCount) {
                    break;
                }
            }
        }
        return database;
    }

    /** TKEH CUD strategy: insert every observed exact 2-item utility in top-k. */
    private void raiseThresholdFromPairs() {
        for (int index = 0; index < pairTable.keys.length; index++) {
            long key = pairTable.keys[index];
            if (key == 0L) {
                continue;
            }
            int left = (int) (key >>> 32);
            int right = (int) key;
            offer(
                    new int[]{newToOld[left], newToOld[right]},
                    pairTable.exactUtilities[index]
            );
        }
    }

    private ParsedLine parse(String line) {
        String[] fields = line.split(":");
        if (fields.length != 3) {
            throw new IllegalArgumentException("Invalid utility transaction: " + line);
        }
        String[] itemTokens = fields[0].trim().split("\\s+");
        String[] utilityTokens = fields[2].trim().split("\\s+");
        if (itemTokens.length != utilityTokens.length) {
            throw new IllegalArgumentException("Item/utility length mismatch: " + line);
        }

        int[] items = new int[itemTokens.length];
        long[] utilities = new long[itemTokens.length];
        for (int index = 0; index < itemTokens.length; index++) {
            items[index] = Integer.parseInt(itemTokens[index]);
            utilities[index] = Long.parseLong(utilityTokens[index]);
            if (utilities[index] < 0L) {
                throw new IllegalArgumentException(
                        "TKEH verifier requires non-negative utilities");
            }
        }
        return new ParsedLine(
                items,
                utilities,
                Long.parseLong(fields[1].trim())
        );
    }

    private boolean isMetadata(String line) {
        if (line == null || line.trim().isEmpty()) {
            return true;
        }
        char first = line.charAt(0);
        return first == '#' || first == '%' || first == '@';
    }

    private void offer(int[] items, long utility) {
        Pattern candidate = new Pattern(items, utility);
        if (queue.size() < topK) {
            queue.offer(candidate);
        } else if (Pattern.WORST_FIRST.compare(candidate, queue.peek()) > 0) {
            queue.poll();
            queue.offer(candidate);
        }
        if (queue.size() == topK) {
            minUtil = queue.peek().utility;
        }
    }

    private Itemsets buildResult() {
        List<Pattern> patterns = new ArrayList<>(queue);
        patterns.sort(Pattern.BEST_FIRST);
        Itemsets result = new Itemsets("Serial TKEH top-" + topK);
        for (Pattern pattern : patterns) {
            result.addItemset(
                    new Itemset(Arrays.copyOf(pattern.items, pattern.items.length), pattern.utility),
                    pattern.items.length
            );
        }
        return result;
    }

    private int[] append(int[] prefix, int item) {
        int[] result = Arrays.copyOf(prefix, prefix.length + 1);
        result[prefix.length] = item;
        return result;
    }

    public void printStats() {
        System.out.println("============= SERIAL TKEH VERIFIER =============");
        System.out.println(" Top-k requested   : " + topK);
        System.out.println(" Results returned  : " + queue.size());
        System.out.println(" Final minUtil     : " + minUtil);
        System.out.println(" Pair CUD/EUCP     : " + activatePairRaising);
        System.out.println(" Observed pairs    : "
                + (pairTable == null ? 0 : pairTable.size));
        System.out.println(" Candidates        : " + candidateCount);
        System.out.println(" Transactions read : " + transactionReadCount);
        System.out.println(" Time ms           : " + (endTimestamp - startTimestamp));
        System.out.println("================================================");
    }

    private static final class FirstScan {
        final Map<Integer, Long> twu = new HashMap<>();
        final Map<Integer, Long> riu = new HashMap<>();
    }

    private static final class ParsedLine {
        final int[] items;
        final long[] utilities;
        final long transactionUtility;

        ParsedLine(int[] items, long[] utilities, long transactionUtility) {
            this.items = items;
            this.utilities = utilities;
            this.transactionUtility = transactionUtility;
        }
    }

    private static final class SerialTransaction {
        final int[] items;
        final long[] utilities;
        final int offset;
        final long prefixUtility;
        final long remainingUtility;

        SerialTransaction(int[] items,
                          long[] utilities,
                          int offset,
                          long prefixUtility,
                          long remainingUtility) {
            this.items = items;
            this.utilities = utilities;
            this.offset = offset;
            this.prefixUtility = prefixUtility;
            this.remainingUtility = remainingUtility;
        }
    }

    private static final class Bounds {
        final long[] lu;
        final long[] su;

        Bounds(int itemCount) {
            lu = new long[itemCount + 1];
            su = new long[itemCount + 1];
        }
    }

    private static final class Projection {
        final List<SerialTransaction> transactions;
        final long utility;
        final Bounds bounds;

        Projection(List<SerialTransaction> transactions, long utility, Bounds bounds) {
            this.transactions = transactions;
            this.utility = utility;
            this.bounds = bounds;
        }
    }

    /** Primitive open-addressing table: one key and two long counters per pair. */
    private static final class PairTable {
        private static final double LOAD_FACTOR = 0.65D;

        long[] keys = new long[16];
        long[] exactUtilities = new long[16];
        long[] twuValues = new long[16];
        int size;
        int resizeAt = (int) (keys.length * LOAD_FACTOR);

        void add(int left,
                 int right,
                 long exactUtility,
                 long transactionUtility) {
            if (size + 1 > resizeAt) {
                resize();
            }
            long key = pairKey(left, right);
            int slot = findSlot(key, keys);
            if (keys[slot] == 0L) {
                keys[slot] = key;
                size++;
            }
            exactUtilities[slot] += exactUtility;
            twuValues[slot] += transactionUtility;
        }

        long getTwu(int left, int right) {
            long key = pairKey(left, right);
            int slot = findSlot(key, keys);
            return keys[slot] == key ? twuValues[slot] : 0L;
        }

        void discardExactUtilities() {
            exactUtilities = null;
        }

        private void resize() {
            if (keys.length >= (1 << 30)) {
                throw new IllegalStateException("Too many distinct item pairs");
            }
            long[] oldKeys = keys;
            long[] oldExact = exactUtilities;
            long[] oldTwu = twuValues;
            int newCapacity = keys.length << 1;
            keys = new long[newCapacity];
            exactUtilities = new long[newCapacity];
            twuValues = new long[newCapacity];
            resizeAt = (int) (newCapacity * LOAD_FACTOR);

            for (int index = 0; index < oldKeys.length; index++) {
                long key = oldKeys[index];
                if (key == 0L) {
                    continue;
                }
                int slot = findSlot(key, keys);
                keys[slot] = key;
                exactUtilities[slot] = oldExact[index];
                twuValues[slot] = oldTwu[index];
            }
        }

        private static int findSlot(long key, long[] table) {
            int mask = table.length - 1;
            int slot = mix(key) & mask;
            while (table[slot] != 0L && table[slot] != key) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private static long pairKey(int left, int right) {
            int first = Math.min(left, right);
            int second = Math.max(left, right);
            return ((long) first << 32) | (second & 0xffffffffL);
        }

        private static int mix(long value) {
            value ^= value >>> 33;
            value *= 0xff51afd7ed558ccdL;
            value ^= value >>> 33;
            value *= 0xc4ceb9fe1a85ec53L;
            value ^= value >>> 33;
            return (int) value;
        }
    }

    private static final class Pattern {
        static final Comparator<Pattern> WORST_FIRST = (left, right) -> {
            int byUtility = Long.compare(left.utility, right.utility);
            return byUtility != 0 ? byUtility : -compareItems(left.items, right.items);
        };
        static final Comparator<Pattern> BEST_FIRST = (left, right) -> {
            int byUtility = Long.compare(right.utility, left.utility);
            return byUtility != 0 ? byUtility : compareItems(left.items, right.items);
        };

        final int[] items;
        final long utility;

        Pattern(int[] items, long utility) {
            this.items = items;
            this.utility = utility;
        }

        private static int compareItems(int[] left, int[] right) {
            int common = Math.min(left.length, right.length);
            for (int index = 0; index < common; index++) {
                int comparison = Integer.compare(left[index], right[index]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(left.length, right.length);
        }
    }
}

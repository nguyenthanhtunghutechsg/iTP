import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/** Runs the independent serial TKEH oracle and compares it with the pool miner. */
public class testCorrectnessParallelVsSerial {
    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "dataset/chainstore.txt";
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        int workers = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        int maximumTransactions = args.length > 3
                ? Integer.parseInt(args[3])
                : Integer.MAX_VALUE;
        boolean directUtilityRaising = args.length > 4
                ? Boolean.parseBoolean(args[4])
                : true;
        boolean bestSuFirst = args.length > 5
                ? Boolean.parseBoolean(args[5])
                : true;
        int maximumOutstanding = args.length > 6
                ? Integer.parseInt(args[6])
                : 128;
        boolean thresholdReady = args.length > 7
                ? Boolean.parseBoolean(args[7])
                : true;
        boolean bestContinuation = args.length > 8
                ? Boolean.parseBoolean(args[8])
                : true;
        int minimumTaskTransactions = args.length > 9
                ? Integer.parseInt(args[9])
                : 1;

        AlgoTKEHSerialVerifier serial = new AlgoTKEHSerialVerifier();
        Itemsets expected = serial.runAlgorithm(k, input, maximumTransactions);

        AlgoEFIM_PTMStyleBaseline dfs = new AlgoEFIM_PTMStyleBaseline();
        dfs.configureTransactionUtilityRaising(false);
        dfs.configureCandidateTaskLimit(maximumOutstanding);
        dfs.configureCandidateTaskMinimumTransactions(minimumTaskTransactions);
        dfs.configureCandidateParallelism(
                false,
                1,
                directUtilityRaising,
                bestSuFirst
        );
        Itemsets dfsResult = dfs.runAlgorithm(
                k,
                input,
                null,
                true,
                maximumTransactions,
                true
        );

        AlgoEFIM_PTMStyleBaseline parallel = new AlgoEFIM_PTMStyleBaseline();
        parallel.configureTransactionUtilityRaising(false);
        parallel.configureCandidateTaskLimit(maximumOutstanding);
        parallel.configureCandidateTaskMinimumTransactions(minimumTaskTransactions);
        parallel.configureCandidateParallelism(
                true,
                workers,
                directUtilityRaising,
                bestSuFirst
        );
        parallel.configureThresholdReadyParallelism(
                thresholdReady,
                bestContinuation
        );
        Itemsets actual = parallel.runAlgorithm(
                k,
                input,
                null,
                true,
                maximumTransactions,
                true
        );

        Map<String, Long> expectedMap = canonical(expected);
        Map<String, Long> dfsMap = canonical(dfsResult);
        Map<String, Long> actualMap = canonical(actual);
        if (!expectedMap.equals(dfsMap)) {
            System.err.println("ORACLE ONLY : " + difference(expectedMap, dfsMap));
            System.err.println("DFS ONLY    : " + difference(dfsMap, expectedMap));
            throw new AssertionError("Recursive DFS result differs from serial TKEH oracle");
        }
        if (!expectedMap.equals(actualMap)) {
            System.err.println("SERIAL ONLY   : " + difference(expectedMap, actualMap));
            System.err.println("PARALLEL ONLY : " + difference(actualMap, expectedMap));
            throw new AssertionError("Parallel result differs from serial TKEH oracle");
        }

        if (input.replace('\\', '/').endsWith("dataset/tkeh_example.txt") && k == 10) {
            verifyPublishedTkehExample(actualMap);
        }

        System.out.println("[CORRECTNESS] PASS | input=" + input
                + " | k=" + k
                + " | patterns=" + actualMap.size()
                + " | workers=" + workers
                + " | directU=" + directUtilityRaising
                + " | bestSU=" + bestSuFirst
                + " | maxOutstanding=" + maximumOutstanding
                + " | minTaskTransactions=" + minimumTaskTransactions
                + " | thresholdReady=" + thresholdReady
                + " | bestContinuation=" + bestContinuation);
        serial.printStats();
        dfs.printStats();
        parallel.printStats();
    }

    private static Map<String, Long> canonical(Itemsets itemsets) {
        Map<String, Long> result = new TreeMap<>();
        for (java.util.List<Itemset> level : itemsets.getLevels()) {
            for (Itemset itemset : level) {
                int[] items = Arrays.copyOf(itemset.getItems(), itemset.size());
                Arrays.sort(items);
                result.put(Arrays.toString(items), (long) itemset.getUtility());
            }
        }
        return result;
    }

    private static Map<String, Long> difference(Map<String, Long> left,
                                                Map<String, Long> right) {
        Map<String, Long> difference = new TreeMap<>();
        for (Map.Entry<String, Long> entry : left.entrySet()) {
            if (!entry.getValue().equals(right.get(entry.getKey()))) {
                difference.put(entry.getKey(), entry.getValue());
            }
        }
        return difference;
    }

    /** Table 11 of the TKEH paper, with item letters mapped A..F to 1..6. */
    private static void verifyPublishedTkehExample(Map<String, Long> actual) {
        Map<String, Long> published = new TreeMap<>();
        published.put("[4, 5]", 37L);
        published.put("[2, 4]", 39L);
        published.put("[2, 4, 5]", 45L);
        published.put("[3, 4, 5]", 37L);
        published.put("[2, 3, 5]", 39L);
        published.put("[2, 3, 5, 6]", 37L);
        published.put("[2, 3, 4, 5]", 37L);
        published.put("[2, 3, 4]", 34L);
        published.put("[2, 5]", 36L);
        published.put("[2, 3, 6]", 34L);
        if (!published.equals(actual)) {
            throw new AssertionError("Result differs from TKEH paper Table 11");
        }
        System.out.println("[PAPER TABLE 11] PASS");
    }
}

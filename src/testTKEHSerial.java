import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Simple runner for the independent, non-parallel TKEH verifier. */
public class testTKEHSerial {
    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : "dataset/chainstore.txt";
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;
        int maximumTransactions = args.length > 2
                ? Integer.parseInt(args[2])
                : Integer.MAX_VALUE;

        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.now();

        File logFile = args.length > 3
                ? new File(args[3])
                : defaultLogFile(input, k, startTime);
        PrintStream console = System.out;
        PrintStream log = new PrintStream(
                new FileOutputStream(logFile, false),
                true,
                StandardCharsets.UTF_8.name()
        );
        console.println("LOG FILE: " + logFile.getAbsolutePath());
        System.setOut(log);
        System.setErr(log);

        try {
            System.out.println("LOG FILE         : " + logFile.getAbsolutePath());
            System.out.println("============= SERIAL TKEH START =============");
            System.out.println(" Start time       : " + startTime.format(formatter));
            System.out.println(" Input            : " + input);
            System.out.println(" Top-k            : " + k);
            System.out.println(" Max transactions : " + maximumTransactions);
            System.out.println(" Max heap MB      : "
                    + Runtime.getRuntime().maxMemory() / 1024 / 1024);
            System.out.println(" Parallel         : false");
            System.out.println(" Pair CUD/EUCP    : true");
            System.out.println("=============================================");

            AlgoTKEHSerialVerifier algorithm = new AlgoTKEHSerialVerifier();
            algorithm.configurePairRaising(true);
            Itemsets topK = algorithm.runAlgorithm(k, input, maximumTransactions);

            printFinalQueue(topK);
            algorithm.printStats();

            System.out.println(" End time         : "
                    + LocalDateTime.now().format(formatter));
            System.out.println(" Status           : SUCCESS");
        } catch (Throwable error) {
            System.err.println(" Status           : ERROR");
            System.err.println(" Error type       : " + error.getClass().getName());
            System.err.println(" Message          : " + error.getMessage());
            error.printStackTrace(System.err);
            throw error;
        } finally {
            log.flush();
        }
    }

    /** Print the final queue globally by descending utility, not grouped by length. */
    private static void printFinalQueue(Itemsets topK) {
        List<Itemset> patterns = new ArrayList<>();
        for (List<Itemset> level : topK.getLevels()) {
            patterns.addAll(level);
        }
        patterns.sort(Comparator
                .comparingDouble(Itemset::getUtility)
                .reversed()
                .thenComparing(testTKEHSerial::itemKey));

        System.out.println();
        System.out.println("============= FINAL TOP-K QUEUE =============");
        System.out.println(" Results returned : " + patterns.size());
        int rank = 1;
        for (Itemset pattern : patterns) {
            int[] items = Arrays.copyOf(pattern.getItems(), pattern.size());
            Arrays.sort(items);
            System.out.println(" #" + rank
                    + " | utility=" + (long) pattern.getUtility()
                    + " | items=" + Arrays.toString(items));
            rank++;
        }
        System.out.println("=============================================");
    }

    private static String itemKey(Itemset itemset) {
        int[] items = Arrays.copyOf(itemset.getItems(), itemset.size());
        Arrays.sort(items);
        return Arrays.toString(items);
    }

    private static File defaultLogFile(String input,
                                       int k,
                                       LocalDateTime startTime) {
        String dataset = new File(input).getName();
        int extension = dataset.lastIndexOf('.');
        if (extension > 0) {
            dataset = dataset.substring(0, extension);
        }
        dataset = dataset.replaceAll("[^a-zA-Z0-9_-]", "_");
        String timestamp = startTime.format(
                DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return new File(dataset
                + "_top_" + k
                + "_serial_tkeh_" + timestamp
                + ".log");
    }
}

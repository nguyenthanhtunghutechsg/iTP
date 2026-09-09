import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// EFIM TESTER - OUTPUT TO FILE
public class testEFIM_PTMChunk {

    public static void main(String[] args)
            throws IOException, InterruptedException {


        String input = "dataset/chainstore.txt";
        int k = 1000000;
//        String input = "db/50M_60I_D20_T95_utility.txt";

        int dbSize = Integer.MAX_VALUE;

        // Experimental switches:
        // - candidateParallelism=false: recursive serial DFS (no candidate pool).
        // - directUtilityRaising=true: calculate U(P ∪ {i}) in the SU scan
        //   and update top-k before candidate i is executed.
        // - bestSUFirst=true: shared pool always takes the largest-SU candidate.
        boolean candidateParallelism = true;
        boolean directUtilityRaising = true;
        boolean bestSUFirst = true;
        boolean transactionUtilityRaising = true;
        boolean thresholdReadyParallelism = true;
        boolean bestChildContinuation = true;
        int maximumOutstandingCandidateTasks = 1024;
        int candidateWorkers = Math.max(
                1,
                8
        );
        int candidatePoolCount = candidateParallelism ? 1 : 0;
        int effectiveWorkers = candidateParallelism ? candidateWorkers : 1;

        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.now();

        redirectConsoleToFile(
                input,
                k,
                candidateParallelism,
                candidatePoolCount,
                effectiveWorkers,
                directUtilityRaising,
                bestSUFirst,
                thresholdReadyParallelism,
                bestChildContinuation,
                maximumOutstandingCandidateTasks,
                startTime
        );

        System.out.println(startTime.format(formatter));

        System.out.println("========================================");
        System.out.println("START TIME    : " + startTime.format(formatter));
        System.out.println("INPUT         : " + input);
        System.out.println("TOP-K         : " + k);
        System.out.println(
                "MAX HEAP MB   : "
                        + Runtime.getRuntime().maxMemory()
                        / 1024
                        / 1024
        );
        System.out.println("CANDIDATE POOL: " + candidateParallelism);
        System.out.println("POOL COUNT    : " + candidatePoolCount);
        System.out.println("POOL WORKERS  : " + effectiveWorkers);
        System.out.println("MAX OUTSTANDING: " + maximumOutstandingCandidateTasks);
        System.out.println("DIRECT U      : " + directUtilityRaising);
        System.out.println("BEST SU FIRST : " + bestSUFirst);
        System.out.println("THRESHOLD READY: " + thresholdReadyParallelism);
        System.out.println("BEST CHILD CONT.: " + bestChildContinuation);
        System.out.println("========================================");

        try {
            AlgoEFIM_PTMStyleBaseline algo =
                    new AlgoEFIM_PTMStyleBaseline();

            algo.configureTransactionUtilityRaising(
                    transactionUtilityRaising
            );
            algo.configureCandidateTaskLimit(
                    maximumOutstandingCandidateTasks
            );
            algo.configureCandidateParallelism(
                    candidateParallelism,
                    candidateWorkers,
                    directUtilityRaising,
                    bestSUFirst
            );
            algo.configureThresholdReadyParallelism(
                    thresholdReadyParallelism,
                    bestChildContinuation
            );

            Itemsets itemsets = algo.runAlgorithm(
                    k,
                    input,
                    null,
                    true,
                    dbSize,
                    true
            );

            algo.printStats();

            LocalDateTime endTime = LocalDateTime.now();

            System.out.println();
            System.out.println("========================================");
            System.out.println("STATUS   : SUCCESS");
            System.out.println("END TIME : " + endTime.format(formatter));
            System.out.println("========================================");

        } catch (Throwable error) {

            LocalDateTime errorTime = LocalDateTime.now();

            System.err.println();
            System.err.println("========================================");
            System.err.println("STATUS     : ERROR");
            System.err.println(
                    "ERROR TIME : " + errorTime.format(formatter)
            );
            System.err.println(
                    "ERROR TYPE : " + error.getClass().getName()
            );
            System.err.println(
                    "MESSAGE    : " + error.getMessage()
            );
            System.err.println("========================================");

            error.printStackTrace(System.err);

            System.out.flush();
            System.err.flush();

            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw (InterruptedException) error;
            }

            if (error instanceof IOException) {
                throw (IOException) error;
            }

            if (error instanceof Error) {
                throw (Error) error;
            }

            if (error instanceof RuntimeException) {
                throw (RuntimeException) error;
            }

            throw new RuntimeException(error);

        } finally {
            System.out.flush();
            System.err.flush();
        }
    }

    /**
     * Chuyển toàn bộ System.out và System.err sang cùng một file.
     *
     * Không dùng BufferedOutputStream để println() được ghi
     * xuống file ngay hơn, tránh mất phần log cuối nếu IntelliJ
     * hoặc JVM bị dừng đột ngột.
     */
    private static void redirectConsoleToFile(String input,
                                              int k,
                                              boolean candidateParallelism,
                                              int poolCount,
                                              int workerCount,
                                               boolean directUtilityRaising,
                                               boolean bestSUFirst,
                                               boolean thresholdReadyParallelism,
                                               boolean bestChildContinuation,
                                               int maximumOutstandingCandidateTasks,
                                              LocalDateTime startTime)
            throws IOException {

        long maxHeapBytes = Runtime.getRuntime().maxMemory();

        String heapName = formatHeapSize(maxHeapBytes);
        String datasetName = datasetName(input);
        String timestamp = startTime.format(
                DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String parallelName = candidateParallelism ? "parallel" : "serial";
        String bsfName = bestSUFirst ? "bsf_on" : "bsf_off";
        String directUName = directUtilityRaising ? "directU_on" : "directU_off";
        String thresholdName = thresholdReadyParallelism ? "ready_on" : "ready_off";
        String continuationName = bestChildContinuation ? "bestcont_on" : "bestcont_off";

        String fileName = datasetName
                + "_" + heapName
                + "_top_" + k
                + "_" + parallelName
                + "_pool_" + poolCount
                + "_workers_" + workerCount
                + "_" + bsfName
                + "_" + directUName
                + "_" + thresholdName
                + "_" + continuationName
                + "_pending_" + maximumOutstandingCandidateTasks
                + "_" + timestamp
                + ".log";

        File logFile = new File(fileName);

        PrintStream logStream = new PrintStream(
                new FileOutputStream(logFile, false),
                true,
                StandardCharsets.UTF_8.name()
        );

        System.setOut(logStream);
        System.setErr(logStream);

        /*
         * Bắt lỗi không được catch từ các thread khác.
         */
        Thread.setDefaultUncaughtExceptionHandler(
                (thread, error) -> {
                    System.err.println();
                    System.err.println(
                            "UNCAUGHT ERROR IN THREAD: "
                                    + thread.getName()
                    );
                    error.printStackTrace(System.err);
                    System.err.flush();
                }
        );

        System.out.println(
                "LOG FILE: " + logFile.getAbsolutePath()
        );
        System.out.flush();
    }

    private static String datasetName(String input) {
        String name = new File(input).getName();
        int extension = name.lastIndexOf('.');
        if (extension > 0) {
            name = name.substring(0, extension);
        }
        return name.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    /**
     * Ví dụ:
     * 1073741824 bytes -> 1g
     * 2147483648 bytes -> 2g
     */
    private static String formatHeapSize(long bytes) {

        double gb =
                bytes / (1024.0 * 1024.0 * 1024.0);

        long roundedGb = Math.round(gb);

        if (roundedGb >= 1
                && Math.abs(gb - roundedGb) < 0.15) {
            return roundedGb + "g";
        }

        long mb = Math.round(
                bytes / (1024.0 * 1024.0)
        );

        return mb + "m";
    }

}


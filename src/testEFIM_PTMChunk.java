import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

// EFIM TESTER - OUTPUT TO FILE
public class testEFIM_PTMChunk {

    private enum ExperimentMode {
        // Genuine recursive DFS; parallelism, MBP and WAD are all off.
        SERIAL_BASELINE(1, false, false, false),
        // Parallel baseline plus memory-bounded projection.
        PARALLEL_MBP(2, true, false, true),
        // Full proposed configuration: parallel + MBP + Work-aware SU.
        PARALLEL_MBP_WAD(3, true, true, true);

        final int cliCode;
        final boolean candidateParallelism;
        final boolean workAwarePriority;
        final boolean memoryBoundedProjection;

        ExperimentMode(int cliCode,
                       boolean candidateParallelism,
                       boolean workAwarePriority,
                       boolean memoryBoundedProjection) {
            this.cliCode = cliCode;
            this.candidateParallelism = candidateParallelism;
            this.workAwarePriority = workAwarePriority;
            this.memoryBoundedProjection = memoryBoundedProjection;
        }

        static ExperimentMode fromCliCode(int code) {
            for (ExperimentMode mode : values()) {
                if (mode.cliCode == code) return mode;
            }
            throw new IllegalArgumentException(
                    "experimentMode must be 1, 2, or 3"
            );
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException {

        // ============================================================
        // IDE EXPERIMENT SETUP
        // Change only this block when preparing a run.
        // ============================================================
        String input = "dataset/50M_5I_8t_max100_utility.txt";
        float scale = 0.5f;
        int[] topKValues = {(int)scale*1000,(int)scale*2000,
                (int)scale*4000,(int)scale*8000,(int)scale*16000};
        int dbSize = Integer.MAX_VALUE;

        ExperimentMode defaultExperimentMode =
                ExperimentMode.SERIAL_BASELINE;
        int defaultCandidateWorkers = 8;
        int maximumOutstandingCandidateTasks = 16;

        boolean objectFreeProjection = true;
        boolean automaticProjectionBudget = true;
        int explicitProjectionBudgetMb = 256;
        int projectionBudgetMb = automaticProjectionBudget
                ? automaticProjectionBudgetMb()
                : explicitProjectionBudgetMb;

        // Adds hot-path counters; keep false for final timed benchmarks.
        boolean diagnosticStatistics = true;
        // ============================================================

        // Optional CLI: testEFIM_PTMChunk <mode:1|2|3> [workers]
        if (args.length > 2) {
            throw new IllegalArgumentException(
                    "Usage: testEFIM_PTMChunk [mode:1|2|3] [workers]"
            );
        }
        ExperimentMode experimentMode = args.length > 0
                ? ExperimentMode.fromCliCode(Integer.parseInt(args[0]))
                : defaultExperimentMode;
        int candidateWorkers = args.length > 1
                ? Integer.parseInt(args[1])
                : defaultCandidateWorkers;
        if (candidateWorkers <= 0) {
            throw new IllegalArgumentException(
                    "candidateWorkers must be greater than 0"
            );
        }

        boolean candidateParallelism = experimentMode.candidateParallelism;
        boolean workAwarePriority = experimentMode.workAwarePriority;
        boolean memoryBoundedProjection =
                experimentMode.memoryBoundedProjection;

        for (int k : topKValues) {
            if (k <= 0) throw new IllegalArgumentException("Every Top-k must be positive");
        }
        if (dbSize <= 0) throw new IllegalArgumentException("Transaction limit must be positive");
        int effectiveWorkers = candidateParallelism ? candidateWorkers : 1;

        for (int i = 0; i < topKValues.length; i++) {
            System.out.println("RUN " + (i + 1) + "/" + topKValues.length
                    + " | TOP-K=" + topKValues[i]);
            runSingle(input, topKValues[i], dbSize, candidateParallelism,
                    experimentMode,
                    workAwarePriority, objectFreeProjection,
                    memoryBoundedProjection, projectionBudgetMb,
                    automaticProjectionBudget,
                    maximumOutstandingCandidateTasks,
                    diagnosticStatistics, candidateWorkers, effectiveWorkers);
        }
    }

    private static void runSingle(String input,
                                  int k,
                                  int dbSize,
                                  boolean candidateParallelism,
                                  ExperimentMode experimentMode,
                                  boolean workAwarePriority,
                                  boolean objectFreeProjection,
                                  boolean memoryBoundedProjection,
                                  int projectionBudgetMb,
                                  boolean automaticProjectionBudget,
                                  int maximumOutstandingCandidateTasks,
                                  boolean diagnosticStatistics,
                                  int candidateWorkers,
                                  int effectiveWorkers)
            throws IOException, InterruptedException {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        Thread.UncaughtExceptionHandler originalHandler =
                Thread.getDefaultUncaughtExceptionHandler();

        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime startTime = LocalDateTime.now();

        RunOutputFiles runFiles = redirectConsoleToRunDirectory(
                input,
                k,
                candidateParallelism,
                effectiveWorkers,
                workAwarePriority,
                objectFreeProjection,
                memoryBoundedProjection,
                projectionBudgetMb,
                automaticProjectionBudget,
                maximumOutstandingCandidateTasks,
                diagnosticStatistics,
                startTime
        );

        System.out.println(startTime.format(formatter));

        System.out.println("========================================");
        System.out.println("START TIME    : " + startTime.format(formatter));
        System.out.println("INPUT         : " + input);
        System.out.println("TOP-K         : " + k);
        System.out.println("EXPERIMENT MODE: " + experimentMode);
        System.out.println(
                "MAX HEAP MB   : "
                        + Runtime.getRuntime().maxMemory()
                        / 1024
                        / 1024
        );
        System.out.println("CANDIDATE POOL: " + candidateParallelism);
        System.out.println("POOL WORKERS  : " + effectiveWorkers);
        System.out.println("TASK ADMISSION : FIXED_"
                + maximumOutstandingCandidateTasks);
        System.out.println("DIAGNOSTICS    : " + diagnosticStatistics);
        System.out.println("WORK-AWARE SU : " + workAwarePriority);
        System.out.println("OBJECT-FREE PROJECTION: " + objectFreeProjection);
        System.out.println("MEMORY-BOUNDED PROJECTION: "
                + (memoryBoundedProjection
                ? projectionBudgetMb + " MB ("
                + (automaticProjectionBudget
                ? "AUTO_25_PERCENT_CAP_512"
                : "EXPLICIT") + ")"
                : "false"));
        System.out.println("THRESHOLD TRACE: "
                + runFiles.thresholdTraceFile.getAbsolutePath());
        System.out.println("========================================");

        AlgoEFIM_PTMStyleBaseline algo =
                new AlgoEFIM_PTMStyleBaseline();
        try {
            algo.configureCandidateTaskLimit(
                    maximumOutstandingCandidateTasks
            );
            algo.configureDiagnosticStatistics(diagnosticStatistics);
            algo.configureObjectFreeProjection(objectFreeProjection);
            algo.configureMemoryBoundedProjection(
                    memoryBoundedProjection,
                    projectionBudgetMb
            );
            algo.configureThresholdTrace(runFiles.thresholdTraceFile);
            algo.configureCandidateParallelism(
                    candidateParallelism,
                    candidateWorkers,
                    workAwarePriority
            );
            algo.runAlgorithm(
                    k,
                    input,
                    null,
                    dbSize
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
            try {
                algo.closeThresholdTrace();
            } finally {
                runFiles.logStream.flush();
                System.setOut(originalOut);
                System.setErr(originalErr);
                Thread.setDefaultUncaughtExceptionHandler(originalHandler);
                runFiles.logStream.close();
            }
        }
    }

    /**
     * Chuyển toàn bộ System.out và System.err sang cùng một file.
     *
     * Không dùng BufferedOutputStream để println() được ghi
     * xuống file ngay hơn, tránh mất phần log cuối nếu IntelliJ
     * hoặc JVM bị dừng đột ngột.
     */
    private static RunOutputFiles redirectConsoleToRunDirectory(
            String input,
            int k,
            boolean candidateParallelism,
            int workerCount,
            boolean workAwarePriority,
            boolean objectFreeProjection,
            boolean memoryBoundedProjection,
            int projectionBudgetMb,
            boolean automaticProjectionBudget,
            int maximumOutstandingCandidateTasks,
            boolean diagnosticStatistics,
            LocalDateTime startTime)
            throws IOException {

        long maxHeapBytes = Runtime.getRuntime().maxMemory();

        String heapName = formatHeapSize(maxHeapBytes);
        String datasetName = datasetName(input);
        String timestamp = startTime.format(
                DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS"));
        String modeName = candidateParallelism
                ? "p" + workerCount
                : "s";

        String runName = datasetName
                + "_" + heapName
                + "_k" + k
                + "_" + modeName
                + "_wad" + (workAwarePriority ? 1 : 0)
                + "_ofp" + (objectFreeProjection ? 1 : 0)
                + (memoryBoundedProjection
                ? "_mbp" + (automaticProjectionBudget ? "a" : "")
                + projectionBudgetMb
                : "")
                + "_q" + maximumOutstandingCandidateTasks
                + "_d" + (diagnosticStatistics ? 1 : 0)
                + "_" + timestamp;

        File resultDirectory = new File("result");
        if (!resultDirectory.isDirectory() && !resultDirectory.mkdirs()) {
            throw new IOException("Cannot create result directory: " + resultDirectory);
        }
        String baseRunName = runName;
        File runDirectory = new File(resultDirectory, runName);
        int collision = 0;
        while (!runDirectory.mkdir()) {
            if (!runDirectory.exists()) {
                throw new IOException(
                        "Cannot create run directory: " + runDirectory
                );
            }
            runName = baseRunName + "_" + (++collision);
            runDirectory = new File(resultDirectory, runName);
        }
        File logFile = new File(runDirectory, runName + ".log");
        File thresholdTraceFile = new File(
                runDirectory,
                runName + "_threshold.csv"
        );

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
        System.out.println(
                "RUN DIRECTORY: " + runDirectory.getAbsolutePath()
        );
        System.out.flush();
        return new RunOutputFiles(
                runDirectory,
                logFile,
                thresholdTraceFile,
                logStream
        );
    }

    private static int automaticProjectionBudgetMb() {
        long maxHeapMb = Runtime.getRuntime().maxMemory() / 1024L / 1024L;
        return (int) Math.max(64L, Math.min(512L, maxHeapMb / 4L));
    }

    private static final class RunOutputFiles {
        final File runDirectory;
        final File logFile;
        final File thresholdTraceFile;
        final PrintStream logStream;

        RunOutputFiles(File runDirectory,
                       File logFile,
                       File thresholdTraceFile,
                       PrintStream logStream) {
            this.runDirectory = runDirectory;
            this.logFile = logFile;
            this.thresholdTraceFile = thresholdTraceFile;
            this.logStream = logStream;
        }
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

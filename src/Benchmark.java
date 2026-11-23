import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Michael: benchmark for task 2. It starts BlockingStream once for every version (original, each fix, serial, more workers),
// reads the first 3 [THROUGHPUT] and [RUNTIME] lines of each run and prints all results with the median at the end
public class Benchmark {
    static final int INTERVALS = 3;
    static final int WARMUP_INTERVALS = 1;
    static final int ROUNDS = 3;
    static Map<String, List<Double>> throughput = new LinkedHashMap<>();
    static Map<String, List<Double>> runtime = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        for (int round = 1; round <= ROUNDS; round++) {
            run("original",                  10, false, false, false, false, false);
            run("+ no isolation call",       10, true,  false, false, false, false);
            run("+ batch",                   10, true,  true,  false, false, false);
            run("+ prepared cache",          10, true,  true,  true,  false, false);
            run("+ IN point reads",          10, true,  true,  true,  true,  false);
            run("+ sums printed",            10, true,  true,  true,  true,  true);
            run("serial / original",           1, false, false, false, false, false);
            run("serial / improved",           1, true,  true,  true,  true,  false);
            for (int workers : new int[] {2, 4, 8, 16}) {
                run("original", workers, false, false, false, false, false);
            }
            for (int workers : new int[] {2, 4, 8, 16}) {
                run("improved", workers, true, true, true, true, false);
            }
        }
        print("THROUGHPUT in tx/s", throughput, "%9.0f");
        print("RUNTIME of one transaction in ms, without connect", runtime, "%9.3f");
    }

    static void run(String name, int numberOfWorkers, boolean skipIsolationLevelCall, boolean sendUpdatesAsOneBatch,
                    boolean reusePreparedStatements, boolean useInClauseForPointReads,
                    boolean printSumsToConsole) throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        Path resultFile = Files.createTempFile("blocking-stream-", ".csv");
        ProcessBuilder builder = new ProcessBuilder(java,
                "-DnumberOfWorkers=" + numberOfWorkers, "-DskipIsolationLevelCall=" + skipIsolationLevelCall,
                "-DsendUpdatesAsOneBatch=" + sendUpdatesAsOneBatch, "-DprintSumsToConsole=" + printSumsToConsole,
                "-DreusePreparedStatements=" + reusePreparedStatements,
                "-DuseInClauseForPointReads=" + useInClauseForPointReads,
                "-DsuppressRetryLogs=true",
                "-DbenchmarkIntervals=" + INTERVALS,
                "-DbenchmarkWarmupIntervals=" + WARMUP_INTERVALS,
                "-DbenchmarkResultFile=" + resultFile,
                "-cp", System.getProperty("java.class.path"), "BlockingStream");
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process stream = builder.start();

        List<Double> throughputSamples = new ArrayList<>();
        List<Double> runtimeSamples = new ArrayList<>();
        int intervals = 0;
        int exitCode = stream.waitFor();
        try {
            if (exitCode == 0) {
                for (String line : Files.readAllLines(resultFile, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    String[] values = line.split(",");
                    if (values.length != 6) {
                        throw new IllegalStateException("Malformed benchmark result: " + line);
                    }
                    throughputSamples.add(Double.parseDouble(values[0]));
                    runtimeSamples.add(
                            Double.parseDouble(values[1])
                            + Double.parseDouble(values[2])
                            + Double.parseDouble(values[3]));
                    intervals++;
                }
            }
        } finally {
            Files.deleteIfExists(resultFile);
        }
        if (intervals != INTERVALS) {
            throw new IllegalStateException("BlockingStream produced no benchmark results (exit code " + exitCode + ")");
        }

        String key = name.startsWith("serial /")
                ? name
                : name + ", " + numberOfWorkers + " workers";
        double throughputMedian = median(throughputSamples);
        double runtimeMedian = median(runtimeSamples);
        throughput.computeIfAbsent(key, k -> new ArrayList<>()).add(throughputMedian);
        runtime.computeIfAbsent(key, k -> new ArrayList<>()).add(runtimeMedian);
        System.out.printf("%-38s %8.0f tx/s %8.3f ms%n",
                key, throughputMedian, runtimeMedian);
    }

    static void print(String title, Map<String, List<Double>> results, String format) {
        System.out.println();
        System.out.println("==== " + title + " (" + ROUNDS + " runs of " + WARMUP_INTERVALS +
                " warm-up + " + INTERVALS + " measured intervals, last column = median) ====");
        for (String name : results.keySet()) {
            List<Double> values = results.get(name);
            StringBuilder line = new StringBuilder(String.format("%-38s", name));
            for (double value : values) {
                line.append(String.format(format, value));
            }
            double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            List<Double> sorted = new ArrayList<>(values);
            Collections.sort(sorted);
            line.append("   |").append(String.format(format, median(sorted)));
            line.append(" (mean ").append(String.format(format, mean)).append(")");
            System.out.println(line);
        }
    }

    static double median(List<Double> values) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Cannot calculate median of empty list");
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        if ((sorted.size() & 1) == 1) {
            return sorted.get(middle);
        }
        return (sorted.get(middle - 1) + sorted.get(middle)) / 2.0;
    }
}

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// Michael: benchmark for task 2. It starts BlockingStream once for every version (original, each fix, serial, more workers),
// reads the first 3 [THROUGHPUT] and [RUNTIME] lines of each run and prints all results with the median at the end
public class Benchmark {
    static final int INTERVALS = 3;
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
            run("serial, original",           1, false, false, false, false, false);
            run("serial, improved",           1, true,  true,  true,  true,  false);
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
        ProcessBuilder builder = new ProcessBuilder(java,
                "-DnumberOfWorkers=" + numberOfWorkers, "-DskipIsolationLevelCall=" + skipIsolationLevelCall,
                "-DsendUpdatesAsOneBatch=" + sendUpdatesAsOneBatch, "-DprintSumsToConsole=" + printSumsToConsole,
                "-DreusePreparedStatements=" + reusePreparedStatements,
                "-DuseInClauseForPointReads=" + useInClauseForPointReads,
                "-cp", System.getProperty("java.class.path"), "BlockingStream");
        builder.redirectErrorStream(true);
        Process stream = builder.start();

        double sum = 0;
        double milliseconds = 0;
        int intervals = 0;
        BufferedReader output = new BufferedReader(new InputStreamReader(stream.getInputStream()));
        String line;
        while (intervals < INTERVALS && (line = output.readLine()) != null) {
            if (line.startsWith("[RUNTIME]")) {
                milliseconds = Double.parseDouble(line.split(" ")[1].replace(',', '.'));
            }
            if (line.startsWith("[THROUGHPUT]")) {
                sum += Double.parseDouble(line.split(" ")[1].replace(',', '.'));
                intervals++;
            }
        }
        stream.destroy();
        stream.waitFor();

        String key = name + ", " + numberOfWorkers + " workers";
        throughput.computeIfAbsent(key, k -> new ArrayList<>()).add(sum / intervals);
        runtime.computeIfAbsent(key, k -> new ArrayList<>()).add(milliseconds);
        System.out.printf("%-38s %8.0f tx/s %8.3f ms%n", key, sum / intervals, milliseconds);
    }

    static void print(String title, Map<String, List<Double>> results, String format) {
        System.out.println();
        System.out.println("==== " + title + " (" + ROUNDS + " runs of " + INTERVALS * 10 + " s each, last column = median) ====");
        for (String name : results.keySet()) {
            List<Double> values = results.get(name);
            StringBuilder line = new StringBuilder(String.format("%-38s", name));
            for (double value : values) {
                line.append(String.format(format, value));
            }
            List<Double> sorted = new ArrayList<>(values);
            Collections.sort(sorted);
            line.append("   |").append(String.format(format, sorted.get(sorted.size() / 2)));
            System.out.println(line);
        }
    }
}

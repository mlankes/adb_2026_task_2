import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;   // Michael: needed for the timers

/**
 *
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
 */

public class BlockingStream {
    //public static int num_taks=30;
    public static int num_queries_per_task = 3;
    private static final int NUM_WORKERS = Integer.getInteger("numberOfWorkers", 10);   // Michael: Benchmark.java can pass -DnumberOfWorkers=1 for the serial run, default is still 10
    // Michael: switches for the bottleneck fixes. Default is on. Benchmark.java turns them off with e.g. -DsendUpdatesAsOneBatch=false
    // all false = behaviour of the original code
    private static final boolean SKIP_ISOLATION_LEVEL_CALL = !"false".equals(System.getProperty("skipIsolationLevelCall"));     // Michael: Bottleneck fix 1, read committed is the default in PostgreSQL, so it is not sent for every transaction
    private static final boolean SEND_UPDATES_AS_ONE_BATCH = !"false".equals(System.getProperty("sendUpdatesAsOneBatch"));       // Michael: Bottleneck fix 2, the 3 updates go to the db in one round trip
    private static final boolean REUSE_PREPARED_STATEMENTS = !"false".equals(System.getProperty("reusePreparedStatements"));
    private static final boolean USE_IN_CLAUSE_FOR_POINT_READS = !"false".equals(System.getProperty("useInClauseForPointReads"));
    private static final boolean PRINT_SUMS_TO_CONSOLE = !"false".equals(System.getProperty("printSumsToConsole"));             // Michael: the task says the sums have to be printed, false for clean console
    private static final boolean SUPPRESS_RETRY_LOGS = "true".equals(System.getProperty("suppressRetryLogs"));
    private static final int BENCHMARK_INTERVALS = Math.max(0, Integer.getInteger("benchmarkIntervals", 0));
    private static final int BENCHMARK_WARMUP_INTERVALS = Math.max(0, Integer.getInteger("benchmarkWarmupIntervals", 1));
    private static final long THROUGHPUT_INTERVAL_MS = 10_000L;
    private static final long BASE_BACKOFF_MS = 10L;     // first backoff window: 0..10 ms
    private static final long MAX_BACKOFF_MS = 1_000L;   // the window never grows beyond 0..1000 ms

    // Michael: added for bottleneck measurements
    final AtomicLong connectNanos = new AtomicLong();
    final AtomicLong setupNanos = new AtomicLong();
    final AtomicLong sqlNanos = new AtomicLong();
    final AtomicLong commitNanos = new AtomicLong();

    final AtomicInteger retries = new AtomicInteger();   // how many aborted attempts were restarted
    final AtomicInteger failures = new AtomicInteger();

    // Before
    // 1-worker Serial throughput: ~70 (between 63 and 73) commits per second
    // 10-worker concurrent: ~180 (between 175 and 185) commits per second
    // 100-worker: ~195 commits per second

    //Connection con;
    Random rand = new Random(123456);
    AtomicInteger num_tuple = new AtomicInteger(50000);
    private static final int INITIAL_TUPLES = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;
    private final AtomicInteger nextTaskId = new AtomicInteger();
    private volatile boolean running = true;
    private ExecutorService executor;

    private static class StatementCache implements AutoCloseable {
        private final Connection connection;
        private final Map<String, PreparedStatement> statements = new HashMap<>();

        StatementCache(Connection connection) {
            this.connection = connection;
        }

        PreparedStatement get(String name) throws SQLException {
            PreparedStatement statement = statements.get(name);
            if (statement != null) {
                return statement;
            }

            String sql;
            switch (name) {
                case "point-read":
                    sql = "SELECT data_value FROM blocking_data WHERE data_id = ?";
                    break;
                case "point-read-in":
                    sql = "SELECT sum(data_value) FROM blocking_data " +
                            "WHERE data_id IN (?, ?, ?)";
                    break;
                case "insert":
                    sql = "INSERT INTO blocking_data VALUES (?, ?, ?)";
                    break;
                case "full-scan":
                    sql = "SELECT sum(data_value) FROM blocking_data";
                    break;
                case "update":
                    sql = "UPDATE blocking_data SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?";
                    break;
                default:
                    throw new IllegalArgumentException("Unknown statement: " + name);
            }

            statement = connection.prepareStatement(sql);
            statements.put(name, statement);
            return statement;
        }

        @Override
        public void close() throws SQLException {
            SQLException failure = null;
            for (PreparedStatement statement : statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    public BlockingStream(){
        tasks_finished = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple.get();i++){
                ps.setInt(1, i);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            //System.out.println(Arrays.toString(results));
            System.out.println("DB has been created");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        //Create DB
        BlockingStream b = new BlockingStream();
        b.createDB();

        b.executor = Executors.newFixedThreadPool(NUM_WORKERS);
        if (REUSE_PREPARED_STATEMENTS) {
            for (int worker = 0; worker < NUM_WORKERS; worker++) {
                b.executor.execute(b.new Worker());
            }
        } else {
            for (int worker = 0; worker < NUM_WORKERS; worker++) {
                b.submitNextTransaction();
            }
        }

        System.out.println("[STARTED] Continuous stream with " + NUM_WORKERS + " workers.");
        try {
            try {
                int interval = 0;
                int totalIntervals = BENCHMARK_INTERVALS + BENCHMARK_WARMUP_INTERVALS;
                while (b.running && (BENCHMARK_INTERVALS == 0 || interval < totalIntervals)) {
                    b.measureThroughput(
                            THROUGHPUT_INTERVAL_MS,
                            BENCHMARK_INTERVALS == 0 || interval >= BENCHMARK_WARMUP_INTERVALS);
                    interval++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            b.running = false;
            b.executor.shutdownNow();
        }
        System.out.println("[DONE] Stream stopped after " + b.tasks_finished + " committed transactions.");
    }

    /**
     * Measures committed transactions per second during one measurement window.
     *
     * @param intervalMillis duration of the measurement window in milliseconds
     * @return committed transactions per second
     * @throws InterruptedException if the benchmark is interrupted while measuring
     */
    public double measureThroughput(long intervalMillis, boolean recordResult) throws InterruptedException {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("The measurement interval must be positive");
        }

        int completedBefore = tasks_finished.get();
        long setupBefore = setupNanos.get();
        long sqlBefore = sqlNanos.get();
        long commitBefore = commitNanos.get();
        int retriesBefore = retries.get();
        int failuresBefore = failures.get();
        long start = System.nanoTime();
        Thread.sleep(intervalMillis);
        long elapsedNanos = System.nanoTime() - start;
        int completedDuringWindow = tasks_finished.get() - completedBefore;
        long setupDuring = setupNanos.get() - setupBefore;
        long sqlDuring = sqlNanos.get() - sqlBefore;
        long commitDuring = commitNanos.get() - commitBefore;
        int retriesDuring = retries.get() - retriesBefore;
        int failuresDuring = failures.get() - failuresBefore;

        double throughput = completedDuringWindow / (elapsedNanos / 1_000_000_000.0);
        int transactionCount = Math.max(1, completedDuringWindow);
        double setupMs = (double) setupDuring / transactionCount / 1_000_000.0;
        double sqlMs = (double) sqlDuring / transactionCount / 1_000_000.0;
        double commitMs = (double) commitDuring / transactionCount / 1_000_000.0;
        double runtimeMs = setupMs + sqlMs + commitMs;

        if (!SUPPRESS_RETRY_LOGS) {
            System.out.printf("[TIME] setup %.3f ms, sql %.3f ms, commit %.3f ms%n",
                    setupMs, sqlMs, commitMs);
            System.out.printf("[RUNTIME] %.3f ms per committed transaction (setup %.3f ms, sql %.3f ms, commit %.3f ms)%n",
                    runtimeMs, setupMs, sqlMs, commitMs);
            System.out.printf("[THROUGHPUT] %.2f committed transactions/second (%d in %.2f seconds), retries: %d, failures: %d%n",
                    throughput, completedDuringWindow, elapsedNanos / 1_000_000_000.0,
                    retriesDuring, failuresDuring);
        }

        String resultFile = System.getProperty("benchmarkResultFile");
        if (recordResult && resultFile != null) {
            try {
                Files.writeString(
                        Path.of(resultFile),
                        String.format(Locale.ROOT, "%.6f,%.6f,%.6f,%.6f,%d,%d%n",
                                throughput, setupMs, sqlMs, commitMs,
                                retriesDuring, failuresDuring),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new IllegalStateException("Could not write benchmark result file: " + resultFile, e);
            }
        }
        return throughput;


    }

    /** Deadlock (40P01) and serialization failure (40001): safe to roll back and run again. */
    private static boolean isRetryable(SQLException e) {
        String state = e.getSQLState();
        return "40P01".equals(state) || "40001".equals(state);
    }

    /** Exponential backoff with "full jitter": random sleep in [0, min(cap, base * 2^attempt)]. */
    private static long backoffMillis(int attempt) {
        long window = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS << Math.min(attempt, 10));
        return ThreadLocalRandom.current().nextLong(window + 1);
    }

    private void submitNextTransaction() {
        if (running) {
            try {
                executor.execute(new Blocker(nextTaskId.getAndIncrement()));
            } catch (RejectedExecutionException e) {
                if (running) {
                    throw e;
                }
            }
        }
    }


    public class Blocker implements Runnable  {
        final int task;
        final double decision = rand.nextDouble();   // transaction type is drawn ONCE, so a retry stays the same type
        private List<Integer> fixedIds;              // tuple ids are drawn once and reused on every retry

        public Blocker(int id){
            this.task = id;
        }

        private List<Integer> selectRandomTupleIds() {
            if (fixedIds != null) return fixedIds;
            fixedIds = new ArrayList<>(num_queries_per_task);
            // Michael: distinct tuples
            while (fixedIds.size() < num_queries_per_task) {
                int id = ThreadLocalRandom.current().nextInt(INITIAL_TUPLES);
                if (!fixedIds.contains(id)) {
                    fixedIds.add(id);
                }
            }
            return fixedIds;
        }

        public void readonly_multipoint_query(Connection con, StatementCache cache) throws SQLException {
            List<Integer> ids = selectRandomTupleIds();
            double sum = 0.0;

            if (USE_IN_CLAUSE_FOR_POINT_READS) {
                PreparedStatement ps = cache != null
                        ? cache.get("point-read-in")
                        : con.prepareStatement(
                                "SELECT sum(data_value) FROM blocking_data " +
                                "WHERE data_id IN (?, ?, ?)");
                for (int i = 0; i < ids.size(); i++) {
                    ps.setInt(i + 1, ids.get(i));
                }
                if (cache == null) {
                    try (PreparedStatement closeable = ps;
                         ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            sum = rs.getDouble(1);
                        }
                    }
                } else {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            sum = rs.getDouble(1);
                        }
                    }
                }
            } else {
                PreparedStatement ps = cache != null
                        ? cache.get("point-read")
                        : con.prepareStatement(
                                "SELECT data_value FROM blocking_data WHERE data_id = ?");
                try {
                    for (int id : ids) {
                        ps.setInt(1, id);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                sum += rs.getDouble("data_value");
                            }
                        }
                    }
                } finally {
                    if (cache == null) {
                        ps.close();
                    } else if (SEND_UPDATES_AS_ONE_BATCH) {
                        ps.clearBatch();
                    }
                }
            }

            if (PRINT_SUMS_TO_CONSOLE) {
                System.out.println("Task " + task + " sum of data_values: " + sum);
            }
        }

        public void insert_query(Connection con, StatementCache cache) throws SQLException {
            PreparedStatement ps = cache != null
                    ? cache.get("insert")
                    : con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)");
            try {
                int id = num_tuple.getAndIncrement(); // Do this client-side since this needs to be exactly one more than the previous maximum id (for now, might be potential for optimization)
                ps.setInt(1, id);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, task);
                ps.executeUpdate();
            } finally {
                if (cache == null) {
                    ps.close();
                }
            }
        }
        public void full_table_scan_query(Connection con, StatementCache cache) throws SQLException {
            PreparedStatement ps = cache != null
                    ? cache.get("full-scan")
                    : con.prepareStatement("SELECT sum(data_value) FROM blocking_data");
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double sum = rs.getDouble(1);
                    if (PRINT_SUMS_TO_CONSOLE) {
                        System.out.println("Task " + task + " sum of data_values: " + sum);
                    }
                }
            } finally {
                if (cache == null) {
                    ps.close();
                }
            }
        }

        public void write_query(Connection con, StatementCache cache) throws SQLException {
            List<Integer> ids = selectRandomTupleIds();

            PreparedStatement ps = cache != null
                    ? cache.get("update")
                    : con.prepareStatement(
                            "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?");
            try {
                for (int id : ids) {
                    ps.setDouble(1, rand.nextDouble() * max_value);
                    ps.setInt(2, task);
                    ps.setInt(3, id);
                    // Michael: Bottleneck fix 2, same 3 updates in the same order, but sent together
                    if (SEND_UPDATES_AS_ONE_BATCH) {        
                        ps.addBatch();
                    } else {
                        ps.executeUpdate();
                    }
                }
                if (SEND_UPDATES_AS_ONE_BATCH) {
                    ps.executeBatch();
                }
            } finally {
                if (cache == null) {
                    ps.close();
                }
            }
        }

        public void run_sql_queries(Connection con, StatementCache cache) throws SQLException {
            long t0 = System.nanoTime();   // Michael: setup timer
            con.setAutoCommit(false);
            // Michael: Bottleneck fix 1, this call costs one round trip for every transaction.
            // read committed is already the default in PostgreSQL, so it can be skipped
            if (!SKIP_ISOLATION_LEVEL_CALL) {
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); // ~25% of run_sql_queries execution time
            }
            long t1 = System.nanoTime();
            setupNanos.addAndGet(t1 - t0);
            if (decision < 0.6) {
                readonly_multipoint_query(con, cache);
            } else if (decision < 0.7) {
                insert_query(con, cache);
            } else if (decision < 0.8) {
                full_table_scan_query(con, cache);
            } else {
                write_query(con, cache);
            }

            sqlNanos.addAndGet(System.nanoTime() - t1);
        }

        public void run() {
            long c0 = System.nanoTime();   // Michael: connect timer
            try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
                connectNanos.addAndGet(System.nanoTime() - c0);
                int attempt = 0;
                while (running) {
                    try {
                        run_sql_queries(con, null);
                        long m0 = System.nanoTime();
                        con.commit();
                        commitNanos.addAndGet(System.nanoTime() - m0);
                        int num_finished = tasks_finished.incrementAndGet();
                        //System.out.println("Done task "+task+" after "+attempt+" retries. I am finisher number "+num_finished);
                        break;
                    } catch (SQLException e) {
                        try {
                            con.rollback();   // releases all locks BEFORE the transaction sleeps
                        } catch (SQLException rollbackError) {
                            e.addSuppressed(rollbackError);
                        }
                        if (!isRetryable(e)) {
                            failures.incrementAndGet();
                            if (!SUPPRESS_RETRY_LOGS) {
                                System.err.println("Task "+task+" failed permanently: "+e.getMessage());
                            }
                            break;
                        }
                        retries.incrementAndGet();
                        long sleepMs = backoffMillis(attempt);
                        attempt++;
                        if (!SUPPRESS_RETRY_LOGS) {
                            System.err.println("Task "+task+" retry "+attempt+" in "+sleepMs+" ms ("+e.getSQLState()+")");
                        }
                        try {
                            Thread.sleep(sleepMs);  // Exponential backoff
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } catch (SQLException e) {
                if (!SUPPRESS_RETRY_LOGS) {
                    System.err.println("Task "+task+" "+e.getMessage());
                }
            } finally {
                submitNextTransaction();
            }
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long c0 = System.nanoTime();
            try (Connection con =
                         ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                 StatementCache cache = new StatementCache(con)) {
                connectNanos.addAndGet(System.nanoTime() - c0);

                while (running) {
                    Blocker transaction = new Blocker(nextTaskId.getAndIncrement());
                    int attempt = 0;

                    while (running) {
                        try {
                            transaction.run_sql_queries(con, cache);
                            long m0 = System.nanoTime();
                            con.commit();
                            commitNanos.addAndGet(System.nanoTime() - m0);
                            tasks_finished.incrementAndGet();
                            break;
                        } catch (SQLException e) {
                            try {
                                con.rollback();
                            } catch (SQLException rollbackError) {
                                e.addSuppressed(rollbackError);
                            }

                            if (!isRetryable(e)) {
                                failures.incrementAndGet();
                                if (!SUPPRESS_RETRY_LOGS) {
                                    System.err.println(
                                            "Task " + transaction.task +
                                            " failed permanently: " + e.getMessage());
                                }
                                break;
                            }

                            retries.incrementAndGet();
                            long sleepMs = backoffMillis(attempt++);
                            if (!SUPPRESS_RETRY_LOGS) {
                                System.err.println(
                                        "Task " + transaction.task +
                                        " retry " + attempt + " in " + sleepMs +
                                        " ms (" + e.getSQLState() + ")");
                            }
                            try {
                                Thread.sleep(sleepMs);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                failures.incrementAndGet();
                if (!SUPPRESS_RETRY_LOGS) {
                    System.err.println("Worker failed: " + e.getMessage());
                }
            }
        }
    }
}

// One bottleneck has been selecting the ids (selectRandomTupleIds) to be used by the multipoint read and write querys
// they made 50% of the runtimes of these querys
// after realizing this function locally as a List of Integers (instead of sql querys causing full table scans),
// we could reduce their runtime share to 2.35% of the querys runtime

// Michael: bottleneck analysis & benchmarks
// 1. timers in the code: 84 % of the time was opening the connection, according to assignment 3 we should not focus on connection related issues?
// 2. EXPLAIN ANALYZE in psql: the full table scan is a Seq Scan over all rows, the read by id is an Index Scan
// 3. the connection is not optimized, the task says so. Fixes: no isolation level call per transaction, the 3 updates as one batch
// 4. inside a transaction almost all time is sql (mostly the full scan), and we are not allowed to change that query

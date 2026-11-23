import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    private static final boolean PRINT_SUMS_TO_CONSOLE = !"false".equals(System.getProperty("printSumsToConsole"));             // Michael: the task says the sums have to be printed, false for clean console
    private static final long THROUGHPUT_INTERVAL_MS = 10_000L;
    private static final long BASE_BACKOFF_MS = 10L;     // first backoff window: 0..10 ms
    private static final long MAX_BACKOFF_MS = 1_000L;   // the window never grows beyond 0..1000 ms

    // Michael: added for bottleneck measurements
    final AtomicLong connectNanos = new AtomicLong();
    final AtomicLong setupNanos = new AtomicLong();
    final AtomicLong sqlNanos = new AtomicLong();
    final AtomicLong commitNanos = new AtomicLong();

    final AtomicInteger retries = new AtomicInteger();   // how many aborted attempts were restarted

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
        for (int worker = 0; worker < NUM_WORKERS; worker++) {
            b.submitNextTransaction();
        }

        System.out.println("[STARTED] Continuous stream with " + NUM_WORKERS + " workers.");
        try {
            try {
                while (b.running) {
                    b.measureThroughput(THROUGHPUT_INTERVAL_MS);
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
    public double measureThroughput(long intervalMillis) throws InterruptedException {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("The measurement interval must be positive");
        }

        int completedBefore = tasks_finished.get();
        long start = System.nanoTime();
        Thread.sleep(intervalMillis);
        long elapsedNanos = System.nanoTime() - start;
        int completedDuringWindow = tasks_finished.get() - completedBefore;

        // Michael: prints where the time goes
        double sum = connectNanos.get() + setupNanos.get() + sqlNanos.get() + commitNanos.get();
        System.out.printf("[TIME] connect %.1f %%, setup %.1f %%, sql %.1f %%, commit %.1f %%%n",
                100 * connectNanos.get() / sum, 100 * setupNanos.get() / sum,
                100 * sqlNanos.get() / sum, 100 * commitNanos.get() / sum);
        // Michael: running time of one transaction without the connection (the task says to focus on this)
        double committed = Math.max(1, tasks_finished.get());
        System.out.printf("[RUNTIME] %.3f ms per transaction without connect (setup %.3f ms, sql %.3f ms, commit %.3f ms)%n",
                (setupNanos.get() + sqlNanos.get() + commitNanos.get()) / committed / 1_000_000.0, setupNanos.get() / committed / 1_000_000.0,
                sqlNanos.get() / committed / 1_000_000.0, commitNanos.get() / committed / 1_000_000.0);
        double throughput = completedDuringWindow / (elapsedNanos / 1_000_000_000.0);
        System.out.printf("[THROUGHPUT] %.2f committed transactions/second (%d in %.2f seconds), total retries so far: %d%n",
                throughput, completedDuringWindow, elapsedNanos / 1_000_000_000.0, retries.get());
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

        public void readonly_multipoint_query(Connection con) throws SQLException {
            List<Integer> ids = selectRandomTupleIds();
            double sum = 0.0;

            try (PreparedStatement ps = con.prepareStatement(
                    "SELECT data_value FROM blocking_data WHERE data_id = ?")) {

                for (int id : ids) {
                    ps.setInt(1, id);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            sum += rs.getDouble("data_value");
                        }
                    }
                }
            }

            if (PRINT_SUMS_TO_CONSOLE) {
                System.out.println("Task " + task + " sum of data_values: " + sum);
            }
        }

        public void insert_query(Connection con) throws SQLException {
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                int id = num_tuple.getAndIncrement(); // Do this client-side since this needs to be exactly one more than the previous maximum id (for now, might be potential for optimization)
                ps.setInt(1, id);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, task);
                ps.executeUpdate();
            }
        }
        public void full_table_scan_query(Connection con) throws SQLException {
            try (PreparedStatement ps = con.prepareStatement("SELECT sum(data_value) FROM blocking_data");
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double sum = rs.getDouble(1);
                    if (PRINT_SUMS_TO_CONSOLE) {
                        System.out.println("Task " + task + " sum of data_values: " + sum);
                    }
                }
            }
        }

        public void write_query(Connection con) throws SQLException {
            List<Integer> ids = selectRandomTupleIds();

            try (PreparedStatement ps = con.prepareStatement(
                    "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?")) {

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
            }
        }

        public void run_sql_queries(Connection con) throws SQLException {   // only 10% of overall runtime
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
                readonly_multipoint_query(con); // ~50% (multipoint query triggers full table scan)
            } else if (decision < 0.7) {
                insert_query(con);              // ~5%
            } else if (decision < 0.8) {
                full_table_scan_query(con);     // ~5%
            } else {
                write_query(con);               // ~15%
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
                        run_sql_queries(con);
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
                            System.err.println("Task "+task+" failed permanently: "+e.getMessage());
                            break;
                        }
                        retries.incrementAndGet();
                        long sleepMs = backoffMillis(attempt);
                        attempt++;
                        System.err.println("Task "+task+" retry "+attempt+" in "+sleepMs+" ms ("+e.getSQLState()+")");
                        try {
                            Thread.sleep(sleepMs);  // Exponential backoff
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("Task "+task+" "+e.getMessage());
            } finally {
                submitNextTransaction();
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

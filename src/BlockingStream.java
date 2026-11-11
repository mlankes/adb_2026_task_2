import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

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
    public static int num_taks=30;
    public static int num_queries_per_task = 3;
    private static final int NUM_WORKERS = 10;
    private static final long THROUGHPUT_INTERVAL_MS = 10_000L;

    Connection con;
    Random rand = new Random(123456);
    AtomicInteger num_tuple = new AtomicInteger(50000);
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
            System.out.println(Arrays.toString(results));
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
        double throughput = completedDuringWindow / (elapsedNanos / 1_000_000_000.0);
        System.out.printf("[THROUGHPUT] %.2f committed transactions/second (%d in %.2f seconds)%n",
                throughput, completedDuringWindow, elapsedNanos / 1_000_000_000.0);
        return throughput;
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

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable  {
        final int task;

        public Blocker(int id){
            this.task = id;
        }

        private List<Integer> selectRandomTupleIds(Connection con) throws SQLException { // might be bottleneck
            String sql = "SELECT data_id FROM blocking_data ORDER BY random() LIMIT ?";
            List<Integer> ids = new ArrayList<>();

            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setInt(1, num_queries_per_task);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ids.add(rs.getInt("data_id"));
                    }
                }
            }

            if (ids.size() < num_queries_per_task) {
                throw new SQLException("Not enough tuples available for the transaction");
            }

            return ids;
        }

        public void readonly_multipoint_query(Connection con) throws SQLException {
            List<Integer> ids = selectRandomTupleIds(con);
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

            System.out.println("Task " + task + " sum of data_values: " + sum);
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
                    System.out.println("Task " + task + " sum of data_values: " + sum);
                }
            }
        }

        public void write_query(Connection con) throws SQLException {
            List<Integer> ids = selectRandomTupleIds(con);

            try (PreparedStatement ps = con.prepareStatement(
                    "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?")) {

                for (int id : ids) {
                    ps.setDouble(1, rand.nextDouble() * max_value);
                    ps.setInt(2, task);
                    ps.setInt(3, id);
                    ps.executeUpdate();
                }
            }
        }

        public void run_sql_queries(Connection con) throws SQLException {
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            double decision = rand.nextDouble();
            if (decision < 0.6) {
                readonly_multipoint_query(con);
            } else if (decision < 0.7) {
                insert_query(con);
            } else if (decision < 0.8) {
                full_table_scan_query(con);
            } else {
                write_query(con);
            }
        }

        @Override
        public void run() {
            try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
                try {
                    run_sql_queries(con);
                    con.commit();
                    int num_finished = tasks_finished.incrementAndGet();
                    System.out.println("Done task "+task+". I am finisher number "+num_finished);
                } catch (SQLException e) {
                    try {
                        con.rollback();
                    } catch (SQLException rollbackError) {
                        e.addSuppressed(rollbackError);
                    }
                    System.err.println("Task "+task+" "+e.getMessage());
                }
            } catch (SQLException e) {
                System.err.println("Task "+task+" "+e.getMessage());
            } finally {
                submitNextTransaction();
            }
        }
    }
}

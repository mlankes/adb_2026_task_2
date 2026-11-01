import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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

public class NoLiveLocks {
    public static int num_taks=30;
    public static int num_queries_per_task = 3;
    private static final long BASE_BACKOFF_MS = 200L;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;

    public NoLiveLocks(){
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

    void cerateDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple;i++){
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

    private boolean isDeadlock(SQLException e) {
        String state = e.getSQLState();
        if (state != null && state.trim().equals("40P01")) return true; // PostgreSQL deadlock SQL state (not all DBs use this)
        String msg = e.getMessage();
        return msg != null && msg.toLowerCase().contains("deadlock"); // generic check (not reliable, maybe delete later)
    }

    public static void main(String[] args){
        //Create DB
        NoLiveLocks b = new NoLiveLocks();
        b.cerateDB();

        //Create task and their queries
        ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
        for(int task=0;task<num_taks;task++){
            my_tasks.add(b.new Blocker(task));
        }

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
        double start, stop;
        start = System.currentTimeMillis();
        for(NoLiveLocks.Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        //wait for the tasks to finish
        while(!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable  {
        final int task;
        final double [] start_range;
        final double [] stop_range;

        public Blocker(int id){
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range  = new double[num_queries_per_task];
            for(int q=0;q<num_queries_per_task;q++) {
                stop_range[q]  = rand.nextDouble()*max_value;
                start_range[q] = rand.nextDouble()*stop_range[q];
            }
        }


        private long computeBackoff(int retries, Random rand) {
            int exponent = Math.max(0, retries);
            double cap = Math.scalb((double) BASE_BACKOFF_MS, exponent);
            if (cap >= Long.MAX_VALUE) {
                return rand.nextLong(Long.MAX_VALUE);
            }
            return rand.nextLong((long) cap + 1);
        }

        @Override
        public void run() {
            int retries = 0;
            while (true) {
                // make use of auto-closeables
                try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                     PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    int q = 0;
                    while(q<num_queries_per_task) {//I want to execute num_queries_per_task update queries
                        ps.setDouble(1, (float) (rand.nextDouble()*max_value));//some value, which one is not important.
                        ps.setDouble(2, start_range[q]);
                        ps.setDouble(3, stop_range[q]);
                        //System.out.println("Started query "+q+" task "+task+" Executing "+ps);
                        ps.execute();
                        q++;
                    }
                    con.commit();
                    int num_finished = tasks_finished.incrementAndGet();
                    System.out.println("Done task "+task+". I am finisher number "+num_finished);
                    break; // exit loop, success
                } catch (SQLException e) {
                    if (isDeadlock(e)) {
                        // Use exponential backoff with jitter to avoid starvation with high probability
                        long backoff = computeBackoff(retries, rand);
                        retries++;
                        Logger.logError("Task " + task + " deadlock error, retrying after backoff of " + backoff + " ms, retry count: " + retries, e);
                        try {
                            Thread.sleep(backoff);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            Logger.logStatus("Task " + task + " interrupted during backoff, exiting.");
                            break;
                        }
                    } else {
                        System.err.println("Task " + task + " non-deadlock error: " + e.getMessage());
                        break;
                    }
                }
            }
        }
    }
}

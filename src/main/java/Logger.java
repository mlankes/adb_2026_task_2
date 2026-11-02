public class Logger {
    public static int deadlock_count = 0;
    public static void logError(String message, Exception e) {
        System.err.println("[ERROR] " + message);
        // e.printStackTrace(System.err);
    }

    public  static void logStatus(String message) {
        System.out.println("[STATUS] " + message);
    }
}

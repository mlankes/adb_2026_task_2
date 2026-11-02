import java.sql.Connection;
import java.sql.SQLException;

import io.github.cdimascio.dotenv.Dotenv;


public class ConnectionFactory {
    public static final int POSTGRESQL = 1;

    public static Connection getDefaultParameterConnection(int dbType) throws SQLException {
        switch (dbType) {
            case POSTGRESQL:
                return getPostgreSQLConnection();
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private ConnectionFactory() {
    }

    private static Connection getPostgreSQLConnection() throws SQLException {
        Connection con = null;
        Dotenv dotenv = Dotenv.load(); // Adjust the path as needed

        String dbName = dotenv.get("DB_NAME");
        String host = dotenv.get("DB_HOST");
        String port = dotenv.get("DB_PORT");
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
        String user = dotenv.get("DB_USER");
        String password = dotenv.get("DB_PASSWORD");
        con = java.sql.DriverManager.getConnection(url, user, password);
        // Logger.logStatus("Connected to PostgreSQL database at " + url + " as user " + user);
        return con;
    }

}

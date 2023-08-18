package HiveExample;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJDBC {

    // private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static final String DRIVER_NAME = "com.amazon.hive.jdbc41.HS2Driver";
    private static final String EMR_DNS = " "; // Insert EMR URL
    private static final String SCHEMA = "default";

    private static final String USERNAME = "hadoop";
    private static final String PASSWORD = "";
    private static final int PORT = 10000; // must enable PORT 10000 in Amazon EC2!!!

    private static final String CONN_STRING = "jdbc:hive2://" + EMR_DNS + ":" + PORT + "/" + SCHEMA;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // Register driver and create driver instance

        Class.forName(DRIVER_NAME);

        Connection con = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from pokes");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + "---" + rs.getString(2));
        }
        System.out.println(con);

        // stmt.executeQuery("CREATE DATABASE userdb");
        // System.out.println("Database userdb created successfully.");
        con.close();

    }
}

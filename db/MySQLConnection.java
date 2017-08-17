package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLConnection {

    private String url;
    private String username;
    private String password;
    private Connection connection;

    public MySQLConnection(Properties db)
    {
        url = db.getProperty("db.url");
        username = db.getProperty("db.username");
        password = db.getProperty("db.password");
    }


    public Connection getConnection () throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(url, username, password);
        }

        return connection;
    }

    public static void checkDriverExistence() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
    }
}

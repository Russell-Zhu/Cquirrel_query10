package org.example.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class JDBCConnector {
    static private final String JDBC_URL = "jdbc:mysql://localhost:3306/tpch_test?useSSL=false&characterEncoding=utf8";
    static private final String JDBC_USER = "root";
    static private final String JDBC_PASSWORD = "123456";

    static private Connection connection = null;

    static {
        try {
            connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static public Connection getConnection() {
        return connection;
    }
}

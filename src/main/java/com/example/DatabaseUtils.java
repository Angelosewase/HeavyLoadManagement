package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseUtils {
    private static final String PROPERTIES_FILE = "db.properties";

    public static Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        try (var inputStream = DatabaseUtils.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            properties.load(inputStream);
        } catch (Exception e) {
            throw new RuntimeException("Error loading database properties", e);
        }

        String url = properties.getProperty("db.url");
        String username = properties.getProperty("db.username");
        String password = properties.getProperty("db.password");

        return DriverManager.getConnection(url, username, password);
    }
}


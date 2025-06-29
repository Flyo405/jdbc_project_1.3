package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.io.InputStream;

public class JdbcClient {
    private Connection connection;
    private final Properties config;

    public JdbcClient() {
        this.config = loadConfig();
    }

    private Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getResourceAsStream("/config.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (Exception e) {
            System.err.println("Ошибка загрузки конфигурации: " + e.getMessage());
        }
        return props;
    }

    public void start() {
        try {
            connection = DriverManager.getConnection(
                    config.getProperty("db.url"),
                    config.getProperty("db.username"),
                    config.getProperty("db.password")
            );

            System.out.println("Подключение установлено, введите SQL выражение");

            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(System.in));
            String sql;
            while ((sql = readLine(reader)) != null) {
                if ("QUIT".equalsIgnoreCase(sql.trim())) {
                    break;
                }
                if (sql.trim().isEmpty()) {
                    continue;
                }
                executeSQL(sql);
            }
            System.out.println("Завершение работы...");
        } catch (SQLException e) {
            System.err.println("Ошибка подключения к БД: " + e.getMessage());
        } finally {
            closeConnection();
        }
    }

    private String readLine(java.io.BufferedReader reader) {
        try {
            return reader.readLine();
        } catch (java.io.IOException e) {
            return null;
        }
    }

    private void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
                System.out.println("Соединение закрыто");
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при закрытии соединения: " + e.getMessage());
        }
    }

    private void executeSQL(String sql) {
        try (Statement statement = connection.createStatement()) {
            boolean isResultSet = statement.execute(sql);

            if (isResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    printResultSet(resultSet);
                }
            } else {
                int updateCount = statement.getUpdateCount();
                System.out.println("Выполнено успешно. Затронуто строк: " + updateCount);
            }
        } catch (SQLException e) {
            System.err.println("Ошибка выполнения SQL: " + e.getMessage());
        }
    }

    private void printResultSet(ResultSet resultSet) throws SQLException {
        java.sql.ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            System.out.printf("%-20s", metaData.getColumnName(i));
        }
        System.out.println();

        int rowCount = 0;
        while (resultSet.next() && rowCount < 10) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-20s", resultSet.getObject(i));
            }
            System.out.println();
            rowCount++;
        }

        if (rowCount == 10 && resultSet.next()) {
            System.out.println("\nВыведено строк: 10\nВ БД есть еще записи");
        } else {
            System.out.println("\nВыведено строк: " + rowCount);
        }
    }
}

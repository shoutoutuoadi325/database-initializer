package com.zhiqi.db;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.StringJoiner;

public class Main {
    public static void main(String[] args) {
        // Update these values accordingly.
        String url = "jdbc:mysql://localhost:3306/dblab1";
        String user = "root";
        String password = "79Haolubenwei";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to database.");

            // 1. Load and execute table creation SQL from table_structure.sql
            try (InputStream sqlStream = Main.class.getClassLoader().getResourceAsStream("table_structure.sql")) {
                if (sqlStream == null) {
                    throw new RuntimeException("table_structure.sql not found in resources.");
                }
                BufferedReader sqlReader = new BufferedReader(new InputStreamReader(sqlStream));
                StringJoiner sqlBuilder = new StringJoiner(" ");
                String line;
                while ((line = sqlReader.readLine()) != null) {
                    sqlBuilder.add(line);
                }
                String ddl = sqlBuilder.toString();
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                    System.out.println("Table created successfully.");
                }
            }

            // 2. Read CSV file and insert data
            try (InputStream csvStream = Main.class.getClassLoader().getResourceAsStream("data.csv")) {
                if (csvStream == null) {
                    throw new RuntimeException("data.csv not found in resources.");
                }
                BufferedReader csvReader = new BufferedReader(new InputStreamReader(csvStream));
                String headerLine = csvReader.readLine();
                if (headerLine == null) {
                    throw new RuntimeException("CSV file is empty.");
                }
                String[] columns = headerLine.split(",");
                String tableName = "test_table"; // Change as required

                // Build INSERT SQL with placeholders
                StringJoiner placeholders = new StringJoiner(", ", "(", ")");
                StringJoiner colNames = new StringJoiner(", ");
                for (String col : columns) {
                    colNames.add(col);
                    placeholders.add("?");
                }
                String insertSql = "INSERT INTO " + tableName + " (" + colNames + ") VALUES " + placeholders;

                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    String dataLine;
                    while ((dataLine = csvReader.readLine()) != null) {
                        if (dataLine.trim().isEmpty())
                            continue;
                        String[] values = dataLine.split(",");
                        for (int i = 0; i < values.length; i++) {
                            // Trim and set all values as strings. Adjust conversion if needed.
                            pstmt.setString(i + 1, values[i].trim());
                        }
                        pstmt.executeUpdate();
                    }
                    System.out.println("Data inserted successfully.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
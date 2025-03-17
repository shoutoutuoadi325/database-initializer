package com.zhiqi.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Scanner;
import java.util.StringJoiner;

public class Main {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/dblab1";
        String user = "root";
        String password = "79Haolubenwei";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to database.");

            // Prompt user to enter CSV file path
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter CSV file path: ");
            String csvPath = scanner.nextLine();

            // Extract table name from CSV file name (without extension)
            File csvFile = new File(csvPath);
            String fileName = csvFile.getName();
            String tableName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;

            // Open CSV file from user-specified path
            try (FileInputStream csvStream = new FileInputStream(csvPath);
                    BufferedReader csvReader = new BufferedReader(new InputStreamReader(csvStream))) {

                // Read header line, which will be used for both table structure and INSERT
                // statement
                String headerLine = csvReader.readLine();
                if (headerLine == null) {
                    throw new RuntimeException("CSV file is empty.");
                }
                String[] columns = headerLine.split(",");
                // Remove surrounding quotes from column names if present
                for (int i = 0; i < columns.length; i++) {
                    columns[i] = columns[i].trim().replaceAll("^\"|\"$", "");
                }

                // 1. Generate and execute table creation SQL from CSV header automatically
                StringJoiner ddlJoiner = new StringJoiner(", ");
                for (String col : columns) {
                    ddlJoiner.add(col + " VARCHAR(255)");
                }
                String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + ddlJoiner.toString() + ");";
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                    System.out
                            .println("Table '" + tableName + "' created successfully with structure from CSV header.");
                }

                // 2. Build INSERT SQL with placeholders
                StringJoiner placeholders = new StringJoiner(", ", "(", ")");
                StringJoiner colNames = new StringJoiner(", ");
                for (String col : columns) {
                    colNames.add(col);
                    placeholders.add("?");
                }
                String insertSql = "INSERT INTO " + tableName + " (" + colNames.toString() + ") VALUES "
                        + placeholders.toString();

                // Insert CSV data rows
                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    String dataLine;
                    while ((dataLine = csvReader.readLine()) != null) {
                        if (dataLine.trim().isEmpty())
                            continue;
                        String[] values = dataLine.split(",");
                        for (int i = 0; i < values.length; i++) {
                            pstmt.setString(i + 1, values[i].trim());
                        }
                        pstmt.executeUpdate();
                    }
                    System.out.println("Data inserted successfully.");
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
package com.zhiqi.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

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
                    int lineNumber = 1; // Start counting after header
                    int emptyValueCount = 0;

                    // Track existing tuples to avoid duplicates
                    Set<String> existingTuples = new HashSet<>();
                    List<Integer> duplicateLineNumbers = new ArrayList<>();

                    while ((dataLine = csvReader.readLine()) != null) {
                        lineNumber++;
                        if (dataLine.trim().isEmpty())
                            continue;

                        // Split the line, preserving empty trailing fields
                        String[] values = dataLine.split(",", -1);

                        // Ensure we have the correct number of values
                        if (values.length < columns.length) {
                            System.out.println("Warning: Line " + lineNumber
                                    + " has fewer columns than expected. Adding NULL values.");
                        }

                        // Build a string representation of the tuple
                        String tupleString = String.join(",", values);

                        // Check if the tuple already exists
                        if (existingTuples.contains(tupleString)) {
                            System.out.println("Duplicate found at line: " + lineNumber + ". Skipping insertion.");
                            duplicateLineNumbers.add(lineNumber);
                            continue; // Skip to the next line
                        }

                        // Set each parameter, using NULL for empty values
                        for (int i = 0; i < columns.length; i++) {
                            String value = (i < values.length) ? values[i].trim() : "";
                            if (value.isEmpty()) {
                                pstmt.setNull(i + 1, Types.VARCHAR);
                                System.out.println("Line " + lineNumber + ": Column '" + columns[i]
                                        + "' is empty, inserting NULL");
                                emptyValueCount++;
                            } else {
                                pstmt.setString(i + 1, value);
                            }
                        }
                        pstmt.executeUpdate();
                        existingTuples.add(tupleString); // Add tuple to the set
                    }
                    System.out.println("Data inserted successfully.");
                    System.out.println("Total empty values converted to NULL: " + emptyValueCount);

                    if (!duplicateLineNumbers.isEmpty()) {
                        System.out
                                .println("The following lines were skipped due to duplicates: " + duplicateLineNumbers);
                    }
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
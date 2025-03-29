package com.zhiqi.db;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class DatabaseImporter {
    private final String url;
    private final String user;
    private final String password;

    public DatabaseImporter(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public void importCSV(String csvPath, Consumer<? super Double> progressCallback,
            Consumer<? super String> messageCallback)
            throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            File csvFile = new File(csvPath);
            String fileName = csvFile.getName();
            String tableName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;

            // Count total lines for progress calculation
            long totalLines = countLines(csvPath) - 1; // Subtract 1 for header
            long currentLine = 0;

            try (BufferedReader csvReader = new BufferedReader(new FileReader(csvPath))) {
                String headerLine = csvReader.readLine();
                if (headerLine == null) {
                    throw new RuntimeException("CSV file is empty.");
                }

                boolean hasQuotedValues = isQuotedCSV(headerLine);
                String[] columns = hasQuotedValues ? parseQuotedCSVLine(headerLine) : headerLine.split(",");

                // Create table
                createTable(conn, tableName, columns);
                messageCallback.accept("Table created successfully");
                progressCallback.accept(0.1);

                // Prepare insert statement
                String insertSql = prepareInsertStatement(tableName, columns);
                Set<String> existingTuples = new HashSet<>();

                try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                    String line;
                    while ((line = csvReader.readLine()) != null) {
                        currentLine++;
                        if (line.trim().isEmpty())
                            continue;

                        String[] values = hasQuotedValues ? parseQuotedCSVLine(line) : line.split(",", -1);
                        insertRow(pstmt, columns, values, existingTuples);

                        // Update progress
                        double progress = 0.1 + (0.9 * currentLine / totalLines);
                        progressCallback.accept(progress);
                        messageCallback.accept(String.format("Importing row %d of %d", currentLine, totalLines));
                    }
                }
            }
        }
    }

    private long countLines(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            return reader.lines().count();
        }
    }

    private void createTable(Connection conn, String tableName, String[] columns) throws SQLException {
        StringJoiner ddlJoiner = new StringJoiner(", ");
        for (String col : columns) {
            ddlJoiner.add(col + " VARCHAR(255)");
        }
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + ddlJoiner.toString() + ");";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    private String prepareInsertStatement(String tableName, String[] columns) {
        StringJoiner placeholders = new StringJoiner(", ", "(", ")");
        StringJoiner colNames = new StringJoiner(", ");
        for (String col : columns) {
            colNames.add(col);
            placeholders.add("?");
        }
        return "INSERT INTO " + tableName + " (" + colNames.toString() + ") VALUES " + placeholders.toString();
    }

    private void insertRow(PreparedStatement pstmt, String[] columns, String[] values, Set<String> existingTuples)
            throws SQLException {
        String tupleString = String.join(",", values);

        if (existingTuples.contains(tupleString)) {
            return; // Skip duplicate
        }

        for (int i = 0; i < columns.length; i++) {
            String value = (i < values.length) ? values[i] : "";
            if (value.isEmpty()) {
                pstmt.setNull(i + 1, Types.VARCHAR);
            } else {
                pstmt.setString(i + 1, value);
            }
        }

        pstmt.executeUpdate();
        existingTuples.add(tupleString);
    }

    private boolean isQuotedCSV(String line) {
        return line.trim().startsWith("\"") && line.contains("\",\"");
    }

    private String[] parseQuotedCSVLine(String line) {
        List<String> result = new ArrayList<>();
        Pattern pattern = Pattern.compile("\"([^\"]*(?:\"\"[^\"]*)*)\"|([^,]+)");
        Matcher matcher = pattern.matcher(line);

        while (matcher.find()) {
            String value = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (value != null) {
                value = value.replace("\"\"", "\"");
                result.add(value);
            } else {
                result.add("");
            }
        }

        int expectedColumns = line.split(",").length;
        while (result.size() < expectedColumns) {
            result.add("");
        }

        return result.toArray(new String[0]);
    }
}
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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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

                // Detect if the CSV file has quoted values by examining the header line
                boolean hasQuotedValues = isQuotedCSV(headerLine);
                System.out.println("CSV format detected: " + (hasQuotedValues ? "Quoted values" : "Unquoted values"));

                // Split the header line and process column names
                String[] columns;
                if (hasQuotedValues) {
                    // For quoted CSVs, we need a more sophisticated approach to handle commas
                    // within quotes
                    columns = parseQuotedCSVLine(headerLine);
                    System.out.println("警告: 程序将从列名和数据值中移除引号包裹，如果引号是数据的一部分，请确保它们被正确地转义！");
                    System.out.println(
                            "Warning: The program will remove surrounding quotes from column names and data values.");
                    System.out.println(
                            "If quotes are part of your actual data, please ensure they are properly escaped in the CSV file!");
                    System.out.println("例如：如果你的数据中包含引号，应该使用双引号进行转义，如 \"\"quoted text\"\" 而不是 \"quoted text\"");
                    System.out.println(
                            "Example: If your data contains quotes, they should be escaped using double quotes, like \"\"quoted text\"\" instead of \"quoted text\"");
                } else {
                    // For unquoted CSVs, simple split by comma is sufficient
                    columns = headerLine.split(",");
                }

                // Keep track of which columns had quotes removed
                boolean[] columnsHadQuotes = new boolean[columns.length];

                // Remove surrounding quotes from column names if present
                boolean quotesRemoved = false;
                for (int i = 0; i < columns.length; i++) {
                    String originalCol = columns[i];
                    columns[i] = columns[i].trim().replaceAll("^\"|\"$", "");
                    if (!originalCol.equals(columns[i])) {
                        quotesRemoved = true;
                        columnsHadQuotes[i] = true;
                    }
                }

                if (quotesRemoved) {
                    System.out.println("\n警告: 列名中的引号被移除了。这可能影响数据导入的准确性。");
                    System.out.println("如果这不是您期望的行为，请检查CSV格式是否正确，或考虑使用其他CSV解析工具。");
                    System.out.println("以下列名被移除了引号：");
                    for (int i = 0; i < columns.length; i++) {
                        if (columnsHadQuotes[i]) {
                            System.out.println("  - 列 " + (i + 1) + ": " + columns[i]);
                        }
                    }
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
                    int quoteRemovalCount = 0;
                    List<String> quoteRemovedExamples = new ArrayList<>(); // Track examples of quote removal

                    // Track existing tuples to avoid duplicates
                    Set<String> existingTuples = new HashSet<>();
                    List<Integer> duplicateLineNumbers = new ArrayList<>();

                    while ((dataLine = csvReader.readLine()) != null) {
                        lineNumber++;
                        if (dataLine.trim().isEmpty())
                            continue;

                        // Parse the data line according to whether it's quoted or not
                        String[] values;
                        if (hasQuotedValues) {
                            values = parseQuotedCSVLine(dataLine);
                        } else {
                            values = dataLine.split(",", -1);
                        }

                        // Remove surrounding quotes from values if present
                        boolean lineHasQuotesRemoved = false;
                        for (int i = 0; i < values.length; i++) {
                            String originalValue = values[i];
                            values[i] = values[i].trim().replaceAll("^\"|\"$", "");
                            if (!originalValue.equals(values[i])) {
                                lineHasQuotesRemoved = true;
                                quoteRemovalCount++;

                                // Store a sample of the quote removal (limited to 5 examples)
                                if (quoteRemovedExamples.size() < 5) {
                                    quoteRemovedExamples.add("行 " + lineNumber + ", 列 '" +
                                            (i < columns.length ? columns[i] : "Unknown") +
                                            "': 从 '" + originalValue + "' 变为 '" + values[i] + "'");
                                }
                            }
                        }

                        if (lineHasQuotesRemoved) {
                            System.out.println("警告: 行 " + lineNumber + " 的数据中引号被移除了。此行数据可能不准确。");
                        }

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
                            String value = (i < values.length) ? values[i] : "";
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

                    if (quoteRemovalCount > 0) {
                        System.out.println("\n警告: 在导入过程中，从 " + quoteRemovalCount + " 个值中移除了引号包裹。");
                        System.out.println("如果这不是您预期的结果，请确保您的数据格式正确，并且如果引号是数据的一部分，请确保它们被正确地转义。");

                        System.out.println("\n引号移除示例：");
                        for (String example : quoteRemovedExamples) {
                            System.out.println("  - " + example);
                        }
                        if (quoteRemovedExamples.size() < quoteRemovalCount) {
                            System.out.println(
                                    "  ... 以及更多 " + (quoteRemovalCount - quoteRemovedExamples.size()) + " 个实例");
                        }

                        System.out.println("\n解决方案：");
                        System.out.println("1. 如果您需要保留引号作为实际数据，请编辑CSV文件，使用双引号转义引号 (\"\")");
                        System.out.println("2. 如果引号只是CSV格式的一部分，不是实际数据，可以忽略此警告");
                        System.out.println("3. 您可以在数据库中查询并验证数据，确认导入是否符合预期");

                        System.out.println(
                                "\nWARNING: Quotes were removed from " + quoteRemovalCount + " values during import.");
                        System.out.println(
                                "If this is unexpected, ensure your data is properly formatted and quotes are escaped if they are part of the data.");
                    }

                    if (!duplicateLineNumbers.isEmpty()) {
                        System.out
                                .println("The following lines were skipped due to duplicates: " + duplicateLineNumbers);
                    }

                    // Add prompt to check data integrity after quote removal
                    if (quoteRemovalCount > 0) {
                        System.out.println("\n建议: 请在数据库中验证导入的数据，确保引号的移除没有影响数据完整性。");
                        System.out.println(
                                "Suggestion: Please verify the imported data in the database to ensure that quote removal has not affected data integrity.");
                    }
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks if the CSV line has quoted values
     * 
     * @param line the CSV line to check
     * @return true if the line has quoted values, false otherwise
     */
    private static boolean isQuotedCSV(String line) {
        // Check if the line starts with a quote and contains comma-separated quoted
        // values
        return line.trim().startsWith("\"") && line.contains("\",\"");
    }

    /**
     * Parses a CSV line that may contain quoted values
     * 
     * @param line the CSV line to parse
     * @return an array of field values from the CSV line
     */
    private static String[] parseQuotedCSVLine(String line) {
        List<String> result = new ArrayList<>();

        // Pattern for quoted fields (handles quotes inside quoted fields)
        Pattern pattern = Pattern.compile("\"([^\"]*(?:\"\"[^\"]*)*)\"|([^,]+)");
        Matcher matcher = pattern.matcher(line);

        while (matcher.find()) {
            // Group 1 is the content inside quotes (if any), otherwise use Group 2
            String value = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (value != null) {
                // Handle escaped quotes (two double quotes become one)
                value = value.replace("\"\"", "\"");
                result.add(value);
            } else {
                // Empty field
                result.add("");
            }
        }

        // Handle case where fields are empty (commas with nothing between them)
        int expectedColumns = line.split(",").length;
        while (result.size() < expectedColumns) {
            result.add("");
        }

        return result.toArray(new String[0]);
    }
}
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;

public class DatabaseInitializer {
    public static void main(String[] args) {
        // ...existing code...
        try (Connection conn = DbUtil.getConnection()) {
            // Step 1: Execute schema SQL to create tables
            String schemaSql = new String(Files.readAllBytes(Paths.get("src/main/resources/schema.sql")));
            try (Statement stmt = conn.createStatement()) {
                // Assume that each SQL command is separated by semicolon
                for (String sql : schemaSql.split(";")) {
                    if (sql.trim().length() > 0) {
                        stmt.execute(sql.trim());
                    }
                }
            }
            // Step 2: Process CSV file and insert data dynamically
            // Example: reading data.csv file from src/main/resources
            File csvFile = new File("src/main/resources/data.csv");
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    throw new RuntimeException("CSV file empty: " + csvFile.getAbsolutePath());
                }
                // split header using comma
                String[] columns = headerLine.split(",");
                // assume table name is same as file name without extension
                String tableName = csvFile.getName().replace(".csv", "");
                // Build dynamic INSERT statement
                StringBuilder placeholders = new StringBuilder();
                for (int i = 0; i < columns.length; i++) {
                    placeholders.append("?");
                    if (i < columns.length - 1) {
                        placeholders.append(",");
                    }
                }
                String sqlInsert = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                        + placeholders.toString() + ")";
                try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",");
                        for (int i = 0; i < values.length; i++) {
                            ps.setString(i + 1, values[i].trim());
                        }
                        ps.executeUpdate();
                    }
                }
            }
            System.out.println("Database initialization complete.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // ...existing code...
    }
}

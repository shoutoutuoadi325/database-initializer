package com.zhiqi.db;

import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.scene.layout.GridPane;
import javafx.geometry.Pos;
import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.Alert.AlertType;

public class DatabaseGUI {
    private TextField urlField;
    private TextField userField;
    private PasswordField passwordField;
    private TextField filePathField;
    private ProgressBar progressBar;
    private Label statusLabel;
    private DatabaseImporter importer;

    public void show(Stage stage) {
        stage.setTitle("CSV Database Importer");

        // Create GUI components
        GridPane grid = new GridPane();
        grid.setAlignment(Pos.CENTER);
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(25, 25, 25, 25));

        // Database connection fields
        urlField = new TextField("jdbc:mysql://localhost:3306/dblab1");
        userField = new TextField("root");
        passwordField = new PasswordField();
        filePathField = new TextField();
        filePathField.setEditable(false);

        // Buttons
        Button browseButton = new Button("Browse CSV File");
        Button importButton = new Button("Import Data");

        // Progress indicators
        progressBar = new ProgressBar(0);
        progressBar.setMaxWidth(Double.MAX_VALUE);
        statusLabel = new Label("Ready");

        // Layout
        grid.add(new Label("Database URL:"), 0, 0);
        grid.add(urlField, 1, 0);
        grid.add(new Label("Username:"), 0, 1);
        grid.add(userField, 1, 1);
        grid.add(new Label("Password:"), 0, 2);
        grid.add(passwordField, 1, 2);
        grid.add(new Label("CSV File:"), 0, 3);
        grid.add(filePathField, 1, 3);
        grid.add(browseButton, 2, 3);

        VBox vbox = new VBox(10);
        vbox.getChildren().addAll(grid, importButton, progressBar, statusLabel);
        vbox.setPadding(new Insets(10));

        // Event handlers
        browseButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("CSV Files", "*.csv"));
            var file = fileChooser.showOpenDialog(stage);
            if (file != null) {
                filePathField.setText(file.getAbsolutePath());
            }
        });

        importButton.setOnAction(e -> {
            if (filePathField.getText().isEmpty()) {
                showAlert("Error", "Please select a CSV file first.");
                return;
            }

            importButton.setDisable(true);
            progressBar.setProgress(0);

            importer = new DatabaseImporter(
                    urlField.getText(),
                    userField.getText(),
                    passwordField.getText());

            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    importer.importCSV(filePathField.getText(), progress -> updateProgress(progress, 1.0),
                            this::updateMessage);
                    return null;
                }
            };

            task.setOnSucceeded(event -> {
                importButton.setDisable(false);
                progressBar.setProgress(1);
                showAlert("Success", "Data imported successfully!");
            });

            task.setOnFailed(event -> {
                importButton.setDisable(false);
                showAlert("Error", "Import failed: " + task.getException().getMessage());
            });

            progressBar.progressProperty().bind(task.progressProperty());
            statusLabel.textProperty().bind(task.messageProperty());

            new Thread(task).start();
        });

        Scene scene = new Scene(vbox);
        stage.setScene(scene);
        stage.show();
    }

    private void showAlert(String title, String content) {
        Platform.runLater(() -> {
            Alert alert = new Alert(AlertType.INFORMATION);
            alert.setTitle(title);
            alert.setHeaderText(null);
            alert.setContentText(content);
            alert.showAndWait();
        });
    }
}

package com.zhiqi.db;

import javafx.application.Application;
import javafx.stage.Stage;

public class Main extends Application {
    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        DatabaseGUI gui = new DatabaseGUI();
        gui.show(primaryStage);
    }
}
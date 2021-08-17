package io.mycat.ui.chart;

import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.ui.MonitorService;
import io.mycat.ui.TableData;
import io.mycat.ui.TableViewOuter;
import io.mycat.util.StringUtil;
import io.vertx.core.Future;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.Duration;

import java.util.*;

public class DatabaseInstanceChart extends Application {
    Timeline timeline = new Timeline();
    TableView<Object> tableView = new TableView<>();
    private String name;
    MonitorService monitorService;

    public static void main(String[] args) {
        launch(args);
    }

    public DatabaseInstanceChart(String name, MonitorService monitorService) {
        this.name = name;
        this.monitorService = monitorService;
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle(name+" 数据库实例监控");

        GridPane root = new GridPane();
        List<Node> items = new ArrayList<>();

        tableView.setPrefWidth(300);
        items.add(tableView);

        VBox controlBox = new VBox();
        TextField rate = new TextField();
        rate.setText("1");
        rate.textProperty().addListener((observable, oldValue, newValue) -> {
            if (!Objects.equals(oldValue, newValue)) {
                play(newValue);
            }
        });
        controlBox.getChildren().addAll(new Label("刷新速率(s)"), rate);
        items.add(controlBox);
        for (int i = 0; i < items.size(); i++) {
            int columnIndex = i % 2;
            int rowIndex = i / 2;
            root.add(items.get(i), columnIndex, rowIndex);
        }


        Scene s = new Scene(root);
        primaryStage.setScene(s);
        primaryStage.show();
        play(rate.getText());
    }

    private void play(String newValue) {
        if (StringUtil.isEmpty(newValue)) {
            return;
        }
        timeline.stop();
        timeline.getKeyFrames().clear();


        timeline.getKeyFrames().add(new KeyFrame(Duration.seconds(Double.parseDouble(newValue)), actionEvent -> {
            Future<DatabaseInstanceEntry.DatabaseInstanceMap> instanceEntryFuture = monitorService.fetchDBEntry();
            instanceEntryFuture.onSuccess(databaseInstanceEntry -> {
                LinkedList<String[]> objects = new LinkedList<>();
                for (Map.Entry<String, DatabaseInstanceEntry.DatabaseInstanceEntry2> entry : databaseInstanceEntry.getDatabaseInstanceMap().entrySet()) {
                    String key = entry.getKey();
                    DatabaseInstanceEntry.DatabaseInstanceEntry2 value = entry.getValue();

                    long qpsSum = value.qps;
                    long con = value.con;
                    long thread = value.thread;

                    objects.add(new String[]{key, qpsSum + "", con + "", thread + ""});
                }

                if (objects.isEmpty()){
                    objects.add(new String[]{"无数据","无数据","无数据","无数据","无数据"});
                }

                Platform.runLater(() -> {
                    TableViewOuter tableViewOuter = new TableViewOuter(tableView);

                    TableData tableData = new TableData(Arrays.asList("name", "qps", "con", "thread"), objects);
                    tableViewOuter.appendData(tableData);
                });

            });

        }));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }
}

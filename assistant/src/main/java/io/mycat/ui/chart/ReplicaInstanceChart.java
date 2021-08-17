package io.mycat.ui.chart;

import io.mycat.monitor.RWEntry;
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

public class ReplicaInstanceChart extends Application {
    Timeline timeline = new Timeline();
    TableView<Object> tableView = new TableView<>();
    private String name;
    MonitorService monitorService;

    public static void main(String[] args) {
        launch(args);
    }

    public ReplicaInstanceChart(String name, MonitorService monitorService) {
        this.name = name;
        this.monitorService = monitorService;
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle(name+" 集群监控");

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
            Future<RWEntry.RWEntryMap> rwEntryFuture = monitorService.fetchRWEntry();
            rwEntryFuture.onSuccess(rwEntry -> {
                LinkedList<String[]> objects = new LinkedList<>();
                for (Map.Entry<String, RWEntry> entry : rwEntry.getRwMap().entrySet()) {
                    String key = entry.getKey();
                    RWEntry rwEntry1 = entry.getValue();
                    String replicaName = key;
                    long master = rwEntry1.getMaster();
                    long slave = rwEntry1.getSlave();
                    boolean status = rwEntry1.isStatus();

                    objects.add(new String[]{replicaName, master + "", slave + "",status?"ok":"need check"});
                }

                if (objects.isEmpty()){
                    objects.add(new String[]{"无数据","无数据","无数据","无数据"});
                }

                Platform.runLater(() -> {
                    TableViewOuter tableViewOuter = new TableViewOuter(tableView);

                    TableData tableData = new TableData(Arrays.asList("name", "master", "slave","status"), objects);
                    tableViewOuter.appendData(tableData);
                });

            });

        }));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }
}

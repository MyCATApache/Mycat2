package io.mycat.ui.chart;

import io.mycat.monitor.InstanceEntry;
import io.mycat.ui.MonitorService;
import io.mycat.util.StringUtil;
import io.vertx.core.Future;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Application;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;
import javafx.util.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InstanceChart extends Application {
    Timeline timeline = new Timeline();
    FlowLineChart cpu = new FlowLineChart();
    FlowLineChart mem = new FlowLineChart();
    ChartVO lqps = new LongTextFieldChart();
    ChartVO pqps = new LongTextFieldChart();
    ChartVO lrt = new LongTextFieldChart();
    ChartVO prt = new LongTextFieldChart();
    ChartVO thread = new LongTextFieldChart();
    ChartVO con = new LongTextFieldChart();
    private String name;
    MonitorService monitorService;
    public static void main(String[] args) {
        launch(args);
    }
    //        Timeline timeline = new Timeline();
//        timeline.getKeyFrames().add(new KeyFrame(Duration.millis(UPDATE_INTERVAL_MS * 3), new EventHandler<ActionEvent>() {
//            @Override
//            public void handle(ActionEvent actionEvent) {
//                double currentMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//            }
//        }));
//        timeline.setCycleCount(Animation.INDEFINITE);
//        animation = new SequentialTransition();
//        animation.getChildren().addAll(timeline);

    public InstanceChart(String name, MonitorService monitorService) {
        this.name = name;
        this.monitorService = monitorService;
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle(name+" 实例监控");

        GridPane root = new GridPane();
        List<Node> items = new ArrayList<>();

        items.add(cpu.createContent("cpu"));
        items.add(mem.createContent("mem"));
        items.add(lqps.createContent("lqps"));
        items.add(pqps.createContent("pqps"));
        items.add(lrt.createContent("lrt"));
        items.add(prt.createContent("prt"));
        items.add(thread.createContent("thread"));
        items.add(con.createContent("con"));

        HBox controlBox = new HBox();
        TextField rate = new TextField();
        rate.setText("1");
        rate.textProperty().addListener((observable, oldValue, newValue) -> {
            if (!Objects.equals(oldValue, newValue)) {
                play(newValue);
            }
        });
        controlBox.getChildren().addAll(new Label("刷新速率(s)"),rate);
        items.add(controlBox);
        for (int i = 0; i < items.size(); i++) {
            int columnIndex = i % 2;
            int rowIndex = i / 2;
            root.add(items.get(i),columnIndex,rowIndex);
        }


        Scene s = new Scene(root);
        primaryStage.setScene(s);
        primaryStage.show();
        play(rate.getText());
    }

    private void play(String newValue) {
        if (StringUtil.isEmpty(newValue)){
            return;
        }
        timeline.stop();
        timeline.getKeyFrames().clear();
        timeline.getKeyFrames().add(new KeyFrame(Duration.seconds(Double.parseDouble(newValue)), actionEvent -> {
            Future<InstanceEntry> instanceEntryFuture = monitorService.fetchInstanceEntry();
            instanceEntryFuture.onSuccess(instanceEntry -> {
                cpu. update(instanceEntry.getCpu());
                mem.update(instanceEntry.getMem());
                lqps.update(instanceEntry.getLqps());
                pqps.update(instanceEntry.getPqps());
                lrt.update(instanceEntry.getLrt());
                prt.update(instanceEntry.getPrt());
                thread.update(instanceEntry.getThread());
                con.update(instanceEntry.getCon());
            });

        }));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }
}

package io.mycat.ui.chart;

import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.Parent;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.scene.paint.Paint;

/**
 * from
 * https://www.cnblogs.com/rojas/p/4720109.html
 */
public class FlowLineChart implements ChartVO{

    private static int MAX_DATA_POINTS = 20;
    private static int LOW_Y_DATA_RANGE = 10;
    private static int TICK_UNIT = 10;

    private XYChart.Series<Number, Number> series;
    private javafx.scene.chart.LineChart<Number, Number> innerLineChart;
    private NumberAxis xAxis = new NumberAxis();
    private NumberAxis yAxis = new NumberAxis();

    private Paint paintXTickLabel;
    private double nextX = 0;
    double currentValueBAK = 0;

    public FlowLineChart() {

    }

    public void update(double value){
        Platform.runLater(()->{
            double drawY = (value - currentValueBAK) / 1000000;
            currentValueBAK = value;
            series.getData().add(new XYChart.Data<Number, Number>(nextX, drawY + 10));
            if (series.getData().size() > MAX_DATA_POINTS) {
                series.getData().remove(0);

            }
            nextX += 1;
            ObservableList<XYChart.Data<Number, Number>> data = series.getData();
            xAxis.setLowerBound(data.get(0).getXValue().doubleValue());
            xAxis.setUpperBound(data.get(data.size() - 1).getXValue().doubleValue());
            innerLineChart.getXAxis().setTickLabelFill(paintXTickLabel);
        });
    }

    public Parent createContent(String title) {

        xAxis = new NumberAxis();
        xAxis.setForceZeroInRange(false);
        xAxis.setAutoRanging(false);
        xAxis.setTickLabelsVisible(false);
        xAxis.setTickMarkVisible(false);
        xAxis.setMinorTickVisible(false);
        xAxis = new NumberAxis(0, LOW_Y_DATA_RANGE + 90, TICK_UNIT / 10);
        yAxis = new NumberAxis(0, LOW_Y_DATA_RANGE + 90, TICK_UNIT / 10);
        yAxis.setAutoRanging(false);

        innerLineChart = new javafx.scene.chart.LineChart<>(xAxis, yAxis);
        innerLineChart.setAnimated(false);
        innerLineChart.setLegendVisible(false);
        innerLineChart.setCreateSymbols(false);
        series = new ScatterChart.Series<>();
        innerLineChart.setTitle(" "+title+" ");
        innerLineChart.getData().add(series);
        paintXTickLabel = xAxis.getTickLabelFill();
        return innerLineChart;
    }
}
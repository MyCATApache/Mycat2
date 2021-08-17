package io.mycat.ui.chart;

import javafx.scene.Parent;

public interface ChartVO {

    void update(double value);

    Parent createContent(String title);
}

package io.mycat.ui.chart;

import javafx.geometry.Pos;
import javafx.scene.Parent;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;

public class LongTextFieldChart implements ChartVO {
    TextField textField = new TextField();

    @Override
    public void update(double value) {
        textField.setText(" " + String.valueOf(Double.valueOf(value).longValue()) + " ");
    }

    @Override
    public Parent createContent(String title) {
        Label label = new Label(title);

        HBox hBox = new HBox();
        hBox.setAlignment(Pos.CENTER);
        hBox.getChildren().add(label);

        hBox.getChildren().add(textField);
        return hBox;
    }
}

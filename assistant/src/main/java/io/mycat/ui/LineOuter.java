package io.mycat.ui;

import javafx.scene.control.TextArea;

import java.util.Objects;

public class LineOuter {
    TextArea output;

    public LineOuter(TextArea output) {
        this.output = output;
    }

    public LineOuter appendLine(Object o){
        output.appendText(  Objects.toString(o));
        output.appendText("\n");
        return this;
    }
}

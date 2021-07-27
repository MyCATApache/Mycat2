package io.mycat.ui;

import javafx.scene.control.TreeCell;

public class SchemaObjectCell extends TreeCell<String> {

    public SchemaObjectCell() {

    }

    @Override
    public void updateItem(String item, boolean empty) {
        super.updateItem(item, empty);
        if (empty) {
            setText(null);
        } else {
            setText(item);
        }

    }
}

package io.mycat.ui;

import javafx.scene.control.TreeCell;

public class SchemaObjectCell extends TreeCell<ObjectItem> {

    public SchemaObjectCell() {

    }

    @Override
    public void updateItem(ObjectItem item, boolean empty) {


        super.updateItem(item, empty);
        if (empty) {
            setText(null);
        } else {
            setText(item.getText());
            System.out.println("id:"+item.getId());
            setId(item.getId());
        }

    }
}

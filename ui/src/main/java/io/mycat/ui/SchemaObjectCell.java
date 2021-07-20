package io.mycat.ui;

import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.ContextMenuBuilder;
import javafx.scene.control.MenuItemBuilder;
import javafx.scene.control.TreeCell;

import java.util.Optional;

import static io.mycat.ui.UIMain.getPath;

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

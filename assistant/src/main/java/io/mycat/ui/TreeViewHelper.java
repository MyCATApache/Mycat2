package io.mycat.ui;

import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;

public class TreeViewHelper {
    TreeView treeView;
    Object selectedItem;

    public TreeViewHelper(TreeView treeView) {
        this.treeView = treeView;
    }

    public <T> TreeItem<T> click(String name) {
        if (selectedItem==null){
            SceneUtil.findNode(treeView, null, name);
            selectedItem = treeView.getSelectionModel().getSelectedItem();
            return (TreeItem) selectedItem;
        }else {
            SceneUtil.findNode(treeView, (TreeItem) selectedItem, name);
            selectedItem = treeView.getSelectionModel().getSelectedItem();
            return (TreeItem) selectedItem;
        }
    }
}

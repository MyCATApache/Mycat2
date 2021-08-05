package io.mycat.ui;

import javafx.beans.NamedArg;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputControl;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;

import java.awt.*;
import java.util.*;
import java.util.function.Supplier;

public class SceneUtil {
    public static Set<Scene> sceneSet = Collections.newSetFromMap(new IdentityHashMap<>());

    public static Scene createScene(@NamedArg("root") Parent root, @NamedArg("width") double width, @NamedArg("height") double height) {
        return createScene(new Scene(root, width, height));
    }

    public static Scene createScene(Supplier<Scene> sceneSupplier) {
        return createScene(sceneSupplier.get());
    }

    public static Scene createScene(Scene scene) {
        sceneSet.add(scene);
        return scene;
    }

    public static void close(Scene scene) {
        sceneSet.remove(scene);
    }

    public static Optional<Node> lookupNode(String query) {
        if(!query.startsWith("#")){
            query = "#"+query;
        }
        for (Scene scene : sceneSet) {
            Node node = scene.lookup(query);
            if (node != null) {
                return Optional.of(node);
            }
        }
        return Optional.empty();
    }

    public static Optional<TextInputControl> lookupTextNode(String query) {
        return lookupNode(query).map(i -> (TextInputControl) i);
    }

    public static void findNode(TreeView<ObjectItem> view, String name) {
        findNode(view, null, name);
    }

    public static void findNode(TreeView<ObjectItem> view, TreeItem<ObjectItem> treeNode, String name) {
        if (treeNode == null) {
            treeNode = view.getRoot();
        }
        if (treeNode.getChildren().isEmpty()) {
            // Do nothing node is empty.
        } else {

            // Loop through each child node.
            for (TreeItem<ObjectItem> node : treeNode.getChildren()) {

                if (node.getValue().equals(name)) {
                    node.setExpanded(true);
                    view.getSelectionModel().select(node);
                } else {
                    node.setExpanded(true);
                }

                // If the current node has children then check them.
                if (!treeNode.getChildren().isEmpty()) {
                    findNode(view, node, name);
                }

            }

        }

    }
}

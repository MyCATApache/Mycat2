package io.mycat.ui;

import com.jfoenix.controls.JFXTextArea;
import com.jfoenix.controls.JFXTreeView;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.JavaFXBuilderFactory;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeView;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import lombok.Data;

@Data
public class Controller {
    @FXML
   private TreeView schemaObjectTree;

    @FXML
    private  TextArea schemaObjectDetail;

    public Controller() {
        System.out.println();
    }

    public TreeView getSchemaObjectTree() {
        return schemaObjectTree;
    }

    public void setSchemaObjectTree(TreeView schemaObjectTree) {
        this.schemaObjectTree = schemaObjectTree;
    }

    public TextArea getSchemaObjectDetail() {
        return schemaObjectDetail;
    }

    public void setSchemaObjectDetail(TextArea schemaObjectDetail) {
        this.schemaObjectDetail = schemaObjectDetail;
    }
}

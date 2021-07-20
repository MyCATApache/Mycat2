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
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import lombok.Data;

@Data
public class Controller {
    public AnchorPane mainPane;
    public MenuBar menu;
    public SplitPane main;
    public TreeView objectTree;
    public TextArea objectText;
    public VBox objectNav;
    public Controller() {
        System.out.println();
    }

}

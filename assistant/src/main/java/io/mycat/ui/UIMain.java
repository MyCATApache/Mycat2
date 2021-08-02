package io.mycat.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.fxml.JavaFXBuilderFactory;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UIMain extends Application {

    public static InfoProviderFactory infoProviderFactory;

    public static InfoProviderFactory getInfoProviderFactory() {
        return infoProviderFactory;
    }

    @Override
    public void start(Stage primaryStage) throws Exception {

        primaryStage.setTitle("Mycat2 UI");

        FXMLLoader fxmlLoader = new FXMLLoader();
        fxmlLoader.setLocation(getClass().getResource("/main.fxml"));
        fxmlLoader.setBuilderFactory(new JavaFXBuilderFactory());
        Parent root = fxmlLoader.load();
        io.mycat.ui.MainPaneVO controller = fxmlLoader.getController();
        controller.getMenu().prefWidthProperty().bind(controller.getMainPane().widthProperty());//菜单自适应

        controller.flashRoot();
        controller.getTabPane().prefWidthProperty().bind(controller.getMainPane().widthProperty());//菜单自适应
//        controller.getTabPane().prefHeightProperty().bind(controller.getMainPane().heightProperty());//菜单自适应
        Scene scene = new Scene(root, 800, 600);
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    public static FXMLLoader loader(String path) {
        FXMLLoader fxmlLoader = new FXMLLoader();
        fxmlLoader.setLocation(UIMain.class.getResource(path));
        fxmlLoader.setBuilderFactory(new JavaFXBuilderFactory());
        return fxmlLoader;
    }

    private void init(TreeView<String> treeView, VBox formCell, TextArea jsonTextArea) {

    }

//
//    private void setFormInfo(GridPane formCell, String name, Object config) {
//        Text scenetitle = new Text(name);
//        formCell.getChildren().clear();
//        formCell.add(scenetitle, 0, 0, 1, 1);
//        ObjectMapper oMapper = new ObjectMapper();
//        Map<String, Object> map = oMapper.convertValue(config, Map.class);
//        Map<String, Object> trs = new HashMap<>();
//        for (Map.Entry<String, Object> stringObjectEntry : map.entrySet()) {
//            Object value = stringObjectEntry.getValue();
//            trs.put(infoProviderFactory.translate(stringObjectEntry.getKey()), value);
//        }
//        setItems(formCell, trs);
//    }

    private void setItems(GridPane formCell, Map<String, Object> map) {
        int index = formCell.getChildren().size();
        for (Map.Entry<String, Object> stringObjectEntry : map.entrySet()) {
            Label key = new Label(stringObjectEntry.getKey());

            Object entryValue = stringObjectEntry.getValue();
            TextField value = new TextField(Optional.ofNullable(entryValue).map(i -> i.toString()).orElse(null));
            formCell.add(key, 0, index);
            formCell.add(value, 1, index);
            index++;
        }
    }

    public static String getPath(TreeItem<String> item) {
        StringBuilder pathBuilder = new StringBuilder();
        for (; item != null; item = item.getParent()) {
            pathBuilder.insert(0, item.getValue());
            pathBuilder.insert(0, "/");
        }
        return pathBuilder.toString();
    }

    public static TreeItem<String> getSchemaViewNode(String name, Map<String, List<String>> map) {
        TreeItem<String> rootItem = new TreeItem<>(name);
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            TreeItem schemaItem = new TreeItem(entry.getKey());
            List<String> strings = entry.getValue();
            strings.forEach(s -> schemaItem.getChildren().add(new TreeItem(s)));
            rootItem.getChildren().add(schemaItem);
        }

        return rootItem;
    }

    public static TreeItem<String> getViewNode(String name, Map<String, List<String>> map) {
        TreeItem<String> rootItem = new TreeItem<>(name);
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            TreeItem schemaItem = new TreeItem(entry.getKey());
            List<String> strings = entry.getValue();
            strings.forEach(s -> schemaItem.getChildren().add(new TreeItem(s)));
            rootItem.getChildren().add(schemaItem);
        }

        return rootItem;
    }

    public static TreeItem<String> getViewNode(String name, List<String> list) {
        TreeItem rootItem = new TreeItem(name);
        for (String s : list) {
            rootItem.getChildren().add(new TreeItem(s));
        }
        return rootItem;
    }



    public static void main(String[] args) {
        launch(args);
    }
}

package io.mycat.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mycat.Partition;
import io.mycat.TableHandler;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.*;
import io.vertx.core.json.Json;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.fxml.JavaFXBuilderFactory;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import javafx.util.Callback;
import lombok.SneakyThrows;

import java.util.*;
import java.util.function.Consumer;

public class UIMain extends Application {

    public static InfoProvider infoProvider;

    @Override
    public void start(Stage primaryStage) throws Exception {
        primaryStage.setTitle("Mycat2 UI");

        FXMLLoader fxmlLoader = new FXMLLoader();
        fxmlLoader.setLocation(getClass().getResource("/main.fxml"));
        fxmlLoader.setBuilderFactory(new JavaFXBuilderFactory());
        Parent root = fxmlLoader.load();
        Controller catalog = fxmlLoader.getController();
        catalog.getMenu().prefWidthProperty().bind(catalog.getMainPane().widthProperty());//菜单自适应
        catalog.getMain().prefWidthProperty().bind(catalog.getMainPane().widthProperty());//菜单自适应


        init(catalog.getObjectTree(), catalog.getObjectNav(), catalog.getObjectText());
        Scene scene = new Scene(root, 300, 275);
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
        TreeItemCellFactory treeItemCellFactory = new TreeItemCellFactory(formCell, jsonTextArea);
        treeView.setShowRoot(false);
        treeView.setCellFactory(treeItemCellFactory);
        TreeItem<String> rootViewNode = getRootViewNode();
        treeView.setRoot(rootViewNode);
    }

//    public void start0(Stage primaryStage) throws Exception {
////        FXMLLoader fxmlLoader = new FXMLLoader();
////        fxmlLoader.setLocation(Objects.requireNonNull(getClass().getClassLoader().getResource("./main.fxml")));
////        fxmlLoader.setBuilderFactory(new JavaFXBuilderFactory());
////        Parent rootNode = fxmlLoader.load();
////        Controller controller= fxmlLoader.getController();
//
//
//        SplitPane mainWindow = new SplitPane();
//        SplitPane leftPanel = new SplitPane();
//
//        SplitPane rightPanel = new SplitPane();
//        mainWindow.getItems().add(leftPanel);
//
//
//        GridPane formCell = new GridPane();
//        formCell.setAlignment(Pos.CENTER);
//        formCell.setHgap(10);
//        formCell.setVgap(10);
//        formCell.setPadding(new Insets(25, 25, 25, 25));
//        mainWindow.getItems().add(formCell);
//
//        mainWindow.getItems().add(rightPanel);
//
//
//        TreeView<String> treeView = new TreeView<>();
//        leftPanel.getItems().add(treeView);
//
//
////        GridPane grid = new GridPane();
////        grid.setAlignment(Pos.CENTER);
////
////        grid.setHgap(10);
////        grid.setVgap(10);
////        grid.setPadding(new Insets(25, 25, 25, 25));
////        Text scenetitle = new Text("Welcome");
////        scenetitle.setFont(Font.font("Tahoma", FontWeight.NORMAL, 20));
////        grid.add(scenetitle, 0, 0, 2, 1);
////
////        Label userName = new Label("User Name:");
////        grid.add(userName, 0, 1);
////
////        TextField userTextField = new TextField();
////        grid.add(userTextField, 1, 1);
////
////        Label pw = new Label("Password:");
////        grid.add(pw, 0, 2);
////
////        PasswordField pwBox = new PasswordField();
////        grid.add(pwBox, 1, 2);
//
//
//        TextArea jsonTextArea = new TextArea();
//        rightPanel.getItems().add(jsonTextArea);
//
//        treeView.setRoot(getRootViewNode());
//        treeView.setShowRoot(false);
//        treeView.setCellFactory(tree -> {
//            TreeCell<String> cell = new TreeCell<String>() {
//                @Override
//                public void updateItem(String item, boolean empty) {
//                    super.updateItem(item, empty);
//                    if (empty) {
//                        setText(null);
//                    } else {
//                        setText(item);
//                    }
//                }
//            };
//            cell.setOnMouseClicked(event -> {
//                if (!cell.isEmpty()) {
//                    TreeItem<String> treeItem = cell.getTreeItem();
//                    Command command = Command.parsePath(getPath(treeItem));
//                    jsonTextArea.setText("");
//                    formCell.getChildren().clear();
//                    Optional<LogicSchemaConfig> logicSchemaConfigOptional = null;
//                    switch (command.getType()) {
//                        case SCHEMA:
//                            logicSchemaConfigOptional = infoProvider.getSchemaConfigByName(command.getSchema());
//                            logicSchemaConfigOptional.ifPresent(c -> {
////                                setFormInfo(formCell, "逻辑库", SchemaConfigVO.from(c));
////                                jsonTextArea.setText(Json.encodePrettily(c));
//                            });
//                            break;
//                        case SHARDING_TABLES:
//                        case GLOBAL_TABLES:
//                        case SINGLE_TABLES:
//                            logicSchemaConfigOptional = infoProvider.getSchemaConfigByName(command.getSchema());
//                            logicSchemaConfigOptional.ifPresent(c -> {
//                                Map<String, ShardingTableConfig> shadingTables = c.getShadingTables();
//                                Map<String, GlobalTableConfig> globalTables = c.getGlobalTables();
//                                Map<String, NormalTableConfig> normalTables = c.getNormalTables();
//                                String tableName = command.getTable();
//                                ShardingTableConfig shardingTableConfig = shadingTables.get(tableName);
//                                GlobalTableConfig globalTableConfig = globalTables.get(tableName);
//                                NormalTableConfig normalTableConfig = normalTables.get(tableName);
////                                if (shardingTableConfig != null) {
////
////                                    Text scenetitle = new Text("逻辑表-分片表");
////                                    formCell.getChildren().clear();
////                                    formCell.add(scenetitle, 0, 0, 1, 1);
////                                    int index = formCell.getChildren().size();
////
////
////                                    Label key = new Label(stringObjectEntry.getKey());
////
////                                    Object entryValue = stringObjectEntry.getValue();
////                                    TextField value = new TextField(Optional.ofNullable(entryValue).map(i -> i.toString()).orElse(null));
////                                    formCell.add(key, 0, index);
////                                    formCell.add(value, 1, index);
////
////                                    for (Map.Entry<String, Object> stringObjectEntry : map.entrySet()) {
////                                        Label key = new Label(stringObjectEntry.getKey());
////
////                                        Object entryValue = stringObjectEntry.getValue();
////                                        TextField value = new TextField(Optional.ofNullable(entryValue).map(i -> i.toString()).orElse(null));
////                                        formCell.add(key, 0, index);
////                                        formCell.add(value, 1, index);
////                                        index++;
////                                    }
////
////                                    jsonTextArea.setText(Json.encodePrettily(c));
////                                } else if (globalTableConfig != null) {
////                                    setFormInfo(formCell, "逻辑表-全局表", globalTableConfig);
////                                    jsonTextArea.setText(Json.encodePrettily(c));
////                                } else if (normalTableConfig != null) {
////                                    setFormInfo(formCell, "逻辑表-单表", normalTableConfig);
////                                    jsonTextArea.setText(Json.encodePrettily(c));
////                                }
//                                ;
//                            });
//                            break;
//                        case CLUSTER:
//                            Optional<ClusterConfig> clusterConfigOptional = infoProvider.getClusterConfigByPath(command.getCluster());
//                            clusterConfigOptional.ifPresent(c -> {
//                                jsonTextArea.setText(Json.encodePrettily(c));
//                            });
//                            break;
//                        case DATASOURCE:
//                            Optional<DatasourceConfig> datasourceConfigOptional = infoProvider.getDatasourceConfigByPath(command.getDatasource());
//                            datasourceConfigOptional.ifPresent(c -> {
//                                jsonTextArea.setText(Json.encodePrettily(c));
//                            });
//                            break;
//                    }
//                    System.out.println("");
//                }
//            });
//            return cell;
//        });
////        treeView.setOnMouseClicked(new EventHandler<MouseEvent>() {
////            @Override
////            public void handle(MouseEvent event) {
////                EventTarget target = event.getTarget();
////                Object source = event.getSource();
////                System.out.println(event);
////            }
////        });
//
//        primaryStage.setTitle("Mycat2 UI");
//        Scene scene = new Scene(mainWindow, 300, 275);
//        primaryStage.setScene(scene);
//        primaryStage.show();
//    }


    private void setFormInfo(GridPane formCell, String name, Object config) {
        Text scenetitle = new Text(name);
        formCell.getChildren().clear();
        formCell.add(scenetitle, 0, 0, 1, 1);
        ObjectMapper oMapper = new ObjectMapper();
        Map<String, Object> map = oMapper.convertValue(config, Map.class);
        Map<String, Object> trs = new HashMap<>();
        for (Map.Entry<String, Object> stringObjectEntry : map.entrySet()) {
            Object value = stringObjectEntry.getValue();
            trs.put(infoProvider.translate(stringObjectEntry.getKey()), value);
        }
        setItems(formCell, trs);
    }

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

    public TreeItem<String> getRootViewNode() {
        TreeItem<String> rootItem = new TreeItem("root");

        List<SchemaHandler> schemas = infoProvider.schemas();
        TreeItem<String> schemaItems = new TreeItem<>("schemas");
        TreeItem<String> clusterItems = new TreeItem<>("clusters");
        TreeItem<String> datasourceItems = new TreeItem<>("datasources");

        rootItem.getChildren().add(schemaItems);
        rootItem.getChildren().add(clusterItems);
        rootItem.getChildren().add(datasourceItems);

        for (SchemaHandler schema : schemas) {

            TreeItem<String> schemaItem = new TreeItem(schema.getName());
            schemaItems.getChildren().add(schemaItem);
            TreeItem<String> shardingTablesItem = new TreeItem("shardingTables");
            TreeItem<String> globalTablesItem = new TreeItem("globalTables");
            TreeItem<String> singleTablesItem = new TreeItem("singleTables");

            schemaItem.getChildren().add(shardingTablesItem);
            schemaItem.getChildren().add(globalTablesItem);
            schemaItem.getChildren().add(singleTablesItem);

            for (TableHandler tableHandler : schema.logicTables().values()) {
                String s = tableHandler.getTableName();
                switch (tableHandler.getType()) {
                    case SHARDING:
                        shardingTablesItem.getChildren().add(new TreeItem(s));
                        break;
                    case GLOBAL:
                        globalTablesItem.getChildren().add(new TreeItem(s));
                        break;
                    case NORMAL:
                        singleTablesItem.getChildren().add(new TreeItem(s));
                        break;
                    case CUSTOM:
                        break;
                }
            }
        }

        for (ClusterConfig cluster : infoProvider.clusters()) {
            clusterItems.getChildren().add(new TreeItem(cluster.getName()));
        }

        for (DatasourceConfig datasourceConfig : infoProvider.datasources()) {
            datasourceItems.getChildren().add(new TreeItem(datasourceConfig.getName()));
        }

        return rootItem;
    }

    public static void main(String[] args) {
        launch(args);
    }
}

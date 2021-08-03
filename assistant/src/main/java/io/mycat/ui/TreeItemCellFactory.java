package io.mycat.ui;

import io.mycat.LogicTableType;
import io.mycat.TableHandler;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.LogicSchemaConfig;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.util.Callback;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.ui.UIMain.getPath;

public class TreeItemCellFactory implements Callback<TreeView<String>, TreeCell<String>> {
    VBox formCell;
    TextArea jsonTextArea;
    private Controller controller;


    public TreeItemCellFactory(Controller controller) {
        this.controller = controller;
        this.formCell = controller.getObjectNav();
        this.jsonTextArea = controller.getObjectText();
    }

    @Override
    public TreeCell<String> call(TreeView<String> tree) {
        TreeCell<String> cell = new SchemaObjectCell();
        cell.setOnMouseClicked(event -> {
            Optional<Command> commandOptional = Command.parsePath(getPath(cell.getTreeItem()));
            if (event != null) {
                if (event.getButton() == MouseButton.SECONDARY) {
                    ContextMenu contextMenu = new ContextMenu();
                    TreeItem<String> treeItem = cell.getTreeItem();
                    if (!treeItem.isLeaf() && treeItem.getParent() == tree.getRoot()) {
                        String value = treeItem.getValue();
                        switch (value) {
                            case "schemas": {
                                MenuItem item1 = new MenuItem("新建逻辑库");
                                item1.setOnAction(event1 -> controller.edit(new LogicSchemaConfig()));
                                contextMenu.getItems().add(item1);
                                break;
                            }
                            case "datasources": {
                                MenuItem item1 = new MenuItem("新建数据源");
                                item1.setOnAction(event1 -> controller.edit(new DatasourceConfig()));
                                contextMenu.getItems().add(item1);
                                break;
                            }
                            case "clusters": {
                                MenuItem item1 = new MenuItem("新建集群");
                                item1.setOnAction(event1 -> controller.edit(new ClusterConfig()));
                                contextMenu.getItems().add(item1);
                                break;
                            }
                        }
                    } else {
                        if (commandOptional.isPresent()) {
                            Command command = commandOptional.get();
                            String schema = command.getSchema();
                            switch (command.getType()) {
                                case SCHEMA: {
                                    MenuItem item1 = new MenuItem("删除逻辑库配置");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除逻辑库配置", "确认删除逻辑库配置");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteLogicalSchema(command.getSchema());
                                            controller.flashRoot();
                                        }
                                    });
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case SHARDING_TABLES: {
                                    MenuItem item1 = new MenuItem("新建分片表");
                                    item1.setOnAction(event1 -> controller.addShardingTable(schema));
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case GLOBAL_TABLES: {
                                    MenuItem item1 = new MenuItem("新建全局表");
                                    item1.setOnAction(event1 -> controller.addGlobalTableConfig(schema));
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case SINGLE_TABLES: {
                                    MenuItem item1 = new MenuItem("新建单表");
                                    item1.setOnAction(event1 -> controller.addNormalTableConfig(schema));
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case SHARDING_TABLE: {
                                    MenuItem item1 = new MenuItem("删除分片表配置");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除分片表", "确认删除分片表配置");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteDatasource(command.getDatasource());
                                            controller.flashDataSource();
                                        }
                                    });
                                    MenuItem item2 = new MenuItem("新建索引表");
                                    item1.setOnAction(event1 -> controller.addGlobalTableConfig(schema));

                                    contextMenu.getItems().add(item1);
                                    contextMenu.getItems().add(item2);
                                    break;
                                }
                                case GLOBAL_TABLE: {
                                    MenuItem item1 = new MenuItem("删除全局表配置");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除全局表配置", "确认删除全局表配置");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteDatasource(command.getDatasource());
                                            controller.flashDataSource();
                                        }
                                    });
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case SINGLE_TABLE: {
                                    MenuItem item1 = new MenuItem("删除单表配置");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除单表配置", "确认删除单表配置");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteDatasource(command.getDatasource());
                                            controller.flashDataSource();
                                        }
                                    });
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case CLUSTER: {
                                    MenuItem item1 = new MenuItem("删除");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除集群", "确认删除集群");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteDatasource(command.getDatasource());
                                            controller.flashDataSource();
                                        }
                                    });
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                                case DATASOURCE: {
                                    MenuItem item1 = new MenuItem("删除");
                                    item1.setOnAction(event1 -> {
                                        boolean doAction = display("删除数据源", "确认删除数据源");
                                        if (doAction) {
                                            controller.getInfoProvider().deleteDatasource(command.getDatasource());
                                            controller.flashDataSource();
                                        }
                                    });
                                    contextMenu.getItems().add(item1);
                                    break;
                                }
                            }

                        }
                    }
                    if (contextMenu != null) {
                        cell.setContextMenu(contextMenu);
                    }
                }
            }
            if (!cell.isEmpty()) {
                controller.clearObjectInfo();
                if (!commandOptional.isPresent()) {
                    return;
                }
                Command command = commandOptional.get();
                switch (command.getType()) {
                    case SCHEMA:
                        Optional<LogicSchemaConfig> logicSchemaConfigOptional = controller.getInfoProvider().getSchemaConfigByName(command.getSchema());
                        logicSchemaConfigOptional.ifPresent(r -> controller.edit(r));
                        break;
                    case SHARDING_TABLE:
                    case GLOBAL_TABLE:
                    case SINGLE_TABLE: {
                        if (command.getSchema() == null || command.getTable() == null) {
                            return;
                        }
                        LogicTableType logicTableType ;
                        String schemaName =command.getSchema();
                        String tableName = command.getTable();

                        switch (command.getType()) {
                            case GLOBAL_TABLE:
                                logicTableType = LogicTableType.GLOBAL;
                                break;
                            case SINGLE_TABLE:
                                logicTableType = LogicTableType.NORMAL;
                                break;
                            case SHARDING_TABLE:
                                logicTableType = LogicTableType.SHARDING;
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }

                        Optional<Object> config = controller.getInfoProvider().getTableConfigByName(command.getSchema(), command.getTable());
                        config.ifPresent(c -> controller.edit(logicTableType,schemaName,tableName,c));


                        break;
                    }
                    case CLUSTER:
                        Optional<ClusterConfig> clusterConfigOptional = controller.getInfoProvider().getClusterConfigByPath(command.getCluster());
                        clusterConfigOptional.ifPresent(c -> controller.edit(c));
                        break;
                    case DATASOURCE:
                        Optional<DatasourceConfig> datasourceConfigOptional = controller.getInfoProvider().getDatasourceConfigByPath(command.getDatasource());
                        datasourceConfigOptional.ifPresent(c -> controller.edit(c));
                        break;
                }
                System.out.println("");
            }
        });
        return cell;
    }

    public static boolean display(String title, String msg) {
        Stage stage = new Stage();
        stage.initModality(Modality.APPLICATION_MODAL);
        Label label = new Label();
        label.setText(msg);
        Button btn1 = new Button("确认");
        Button btn2 = new Button("取消");
        AtomicBoolean flag = new AtomicBoolean();
        btn1.setOnMouseClicked(event -> {
            flag.set(true);
            System.out.println("你点击了是");
            stage.close();
        });
        btn2.setOnMouseClicked(event -> {
            flag.set(false);
            System.out.println("你点击了否");
            stage.close();
        });
        HBox hBox = new HBox();
        hBox.getChildren().addAll(btn1, btn2);
        //设置居中
        hBox.setAlignment(Pos.CENTER);

        VBox vBox = new VBox();
        vBox.getChildren().add(label);
        vBox.getChildren().add(hBox);
        vBox.setAlignment(Pos.CENTER);

        Scene scene = new Scene(vBox, 400, 200);
        stage.setScene(scene);
        stage.setTitle(title);
        stage.showAndWait();

        return flag.get();
    }

    public static boolean saveBox(String title, Node node) {
        Stage stage = new Stage();
        stage.initModality(Modality.APPLICATION_MODAL);
        Label label = new Label();
        label.setText(title);
        Button btn1 = new Button("确认");
        Button btn2 = new Button("取消");
        AtomicBoolean flag = new AtomicBoolean();
        btn1.setOnMouseClicked(event -> {
            flag.set(true);
            System.out.println("你点击了是");
            stage.close();
        });
        btn2.setOnMouseClicked(event -> {
            flag.set(false);
            System.out.println("你点击了否");
            stage.close();
        });

        VBox vBox = new VBox();
        vBox.getChildren().add(label);
        vBox.getChildren().add(node);
        vBox.setAlignment(Pos.CENTER);

        Scene scene = new Scene(vBox, 400, 400);
        stage.setScene(scene);
        stage.setTitle(title);
        stage.showAndWait();

        return flag.get();
    }
}

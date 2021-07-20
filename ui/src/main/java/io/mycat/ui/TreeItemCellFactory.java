package io.mycat.ui;

import io.mycat.Partition;
import io.mycat.TableHandler;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.*;
import io.vertx.core.json.Json;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.VBox;
import javafx.scene.shape.Box;
import javafx.util.Callback;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.mycat.ui.UIMain.getPath;
import static io.mycat.ui.UIMain.infoProvider;

public class TreeItemCellFactory implements Callback<TreeView<String>, TreeCell<String>> {
    VBox formCell;
    TextArea jsonTextArea;

    public TreeItemCellFactory(VBox formCell, TextArea jsonTextArea) {
        this.formCell = formCell;
        this.jsonTextArea = jsonTextArea;
    }

    @Override
    public TreeCell<String> call(TreeView<String> tree) {
        TreeCell<String> cell = new SchemaObjectCell();


        cell.setOnMouseClicked(event -> {
            Optional<Command> commandOptional = Command.parsePath(getPath(cell.getTreeItem()));
            if (event != null) {

                if (event.getButton() == MouseButton.SECONDARY) {
                    ContextMenu contextMenu = null;
                    TreeItem<String> treeItem = cell.getTreeItem();
                    if (!treeItem.isLeaf() && treeItem.getParent() == tree.getRoot()) {
                        String value = treeItem.getValue();
                        switch (value) {
                            case "schemas": {
                                contextMenu = ContextMenuBuilder.create()
                                        .items(
                                                MenuItemBuilder.create()
                                                        .text("新建逻辑库")
                                                        .onAction(
                                                                new EventHandler() {
                                                                    @Override
                                                                    public void handle(Event arg0) {
                                                                        System.out.println("Menu Item Clicked!");
                                                                    }
                                                                }
                                                        )
                                                        .build()
                                        )
                                        .build();
                                break;
                            }
                            case "datasources": {
                                contextMenu = ContextMenuBuilder.create()
                                        .items(
                                                MenuItemBuilder.create()
                                                        .text("新建数据源")
                                                        .onAction(
                                                                new EventHandler() {
                                                                    @Override
                                                                    public void handle(Event arg0) {
                                                                        System.out.println("Menu Item Clicked!");
                                                                    }
                                                                }
                                                        )
                                                        .build()
                                        )
                                        .build();
                                break;
                            }
                            case "clusters": {
                                contextMenu = ContextMenuBuilder.create()
                                        .items(
                                                MenuItemBuilder.create()
                                                        .text("新建集群")
                                                        .onAction(
                                                                new EventHandler() {
                                                                    @Override
                                                                    public void handle(Event arg0) {
                                                                        System.out.println("Menu Item Clicked!");
                                                                    }
                                                                }
                                                        )
                                                        .build()
                                        )
                                        .build();
                                break;
                            }
                        }
                    }else{
                      if(  commandOptional.isPresent()){
                          Command command = commandOptional.get();

                          switch (command.getType()) {
                              case SCHEMA:
                              case SHARDING_TABLES:
                              case GLOBAL_TABLES:
                              case SINGLE_TABLES:
                                  contextMenu = ContextMenuBuilder.create()
                                          .items(
                                                  MenuItemBuilder.create()
                                                          .text("新建表")
                                                          .onAction(
                                                                  new EventHandler() {
                                                                      @Override
                                                                      public void handle(Event arg0) {
                                                                          System.out.println("Menu Item Clicked!");
                                                                      }
                                                                  }
                                                          )
                                                          .build()
                                          )
                                          .build();
                                  break;
                              case SHARDING_TABLE:
                              case GLOBAL_TABLE:
                              case SINGLE_TABLE:
                              case CLUSTER:
                              case DATASOURCE:
                                  contextMenu = ContextMenuBuilder.create()
                                          .items(
                                                  MenuItemBuilder.create()
                                                          .text("删除")
                                                          .onAction(
                                                                  new EventHandler() {
                                                                      @Override
                                                                      public void handle(Event arg0) {
                                                                          System.out.println("Menu Item Clicked!");
                                                                      }
                                                                  }
                                                          )
                                                          .build()
                                          )
                                          .build();
                          }

                      }
                    }
                    if (contextMenu != null) {
                        cell.setContextMenu(contextMenu);
                    }
                }
            }
            if (!cell.isEmpty()) {

                jsonTextArea.setText("");
                formCell.getChildren().clear();
                //formCell.getChildren().clear();

                if (!commandOptional.isPresent()) {
                    return;
                }
                Command command = commandOptional.get();
                switch (command.getType()) {
                    case SCHEMA:
                        Optional<LogicSchemaConfig> logicSchemaConfigOptional = infoProvider.getSchemaConfigByName(command.getSchema());
                        logicSchemaConfigOptional.ifPresent(new Consumer<LogicSchemaConfig>() {
                            @Override
                            @SneakyThrows
                            public void accept(LogicSchemaConfig r) {
                                FXMLLoader loader = UIMain.loader("/schema.fxml");
                                Parent parent = loader.load();
                                SchemaConfigVO schemaConfigVO = loader.getController();
                                schemaConfigVO.getSchemaName().setText(r.getSchemaName());
                                schemaConfigVO.getDefaultTargetName().setText(r.getTargetName());
                                formCell.getChildren().add(parent);
                                jsonTextArea.setText(Json.encodePrettily(r));
                            }
                        });
                        break;
                    case SHARDING_TABLE:
                    case GLOBAL_TABLE:
                    case SINGLE_TABLE:
                        if (command.getSchema() == null || command.getTable() == null) {
                            return;
                        }
                        Optional<TableHandler> config = infoProvider.getTableConfigByName(command.getSchema(), command.getTable());
                        config.ifPresent(new Consumer<TableHandler>() {
                            @Override
                            @SneakyThrows
                            public void accept(TableHandler c) {
                                switch (c.getType()) {
                                    case SHARDING: {
                                        ShardingTable shardingTable = (ShardingTable) c;
                                        FXMLLoader loader = UIMain.loader("/shardingTable.fxml");
                                        Parent parent = loader.load();

                                        ShardingTableConfigVO shardingTableConfigVO = loader.getController();
                                        shardingTableConfigVO.getSchemaName().setText(command.getSchema());
                                        shardingTableConfigVO.getTableName().setText(command.getTable());

                                        ShardingTableConfig shardingTableConfig = shardingTable.getTableConfigConfig();
                                        shardingTableConfigVO.getShardingInfo().setText(Json.encodePrettily(shardingTableConfig.getFunction()));

                                        TableView partitionsView = shardingTableConfigVO.getPartitionsView();
                                        for (Partition backend : shardingTable.getBackends()) {
                                            partitionsView.getItems().add(PartitionEntry.of(backend));
                                        }

                                        TableColumn firstCol = new TableColumn("目标");
                                        TableColumn secondCol = new TableColumn("物理分库");
                                        TableColumn thirdCol = new TableColumn("物理分表");


                                        firstCol.setCellValueFactory(
                                                new PropertyValueFactory<Partition, String>("target")
                                        );
                                        secondCol.setCellValueFactory(
                                                new PropertyValueFactory<Partition, String>("schema")
                                        );
                                        thirdCol.setCellValueFactory(
                                                new PropertyValueFactory<Partition, String>("table")
                                        );

                                        partitionsView.getColumns().addAll(firstCol, secondCol, thirdCol);


                                        formCell.getChildren().add(parent);
                                        jsonTextArea.setText(Json.encodePrettily(shardingTableConfig));

                                        break;
                                    }
                                    case GLOBAL: {
                                        GlobalTable globalTable = (GlobalTable) c;
                                        FXMLLoader loader = UIMain.loader("/globalTable.fxml");
                                        Parent parent = loader.load();

                                        GlobalTableConfigVO controller = loader.getController();
                                        controller.getSchemaName().setText(command.getSchema());
                                        controller.getTableName().setText(command.getTable());

                                        ListView tableView = controller.getTargets();

                                        GlobalTableConfig globalTableConfig = new GlobalTableConfig();
                                        globalTableConfig.setCreateTableSQL(globalTable.getCreateTableSQL());
                                        List<GlobalBackEndTableInfoConfig> globalBackEndTableInfoConfigs = new ArrayList<>();
                                        for (Partition partition : globalTable.getGlobalDataNode()) {
                                            GlobalBackEndTableInfoConfig globalBackEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                                            globalBackEndTableInfoConfig.setTargetName(partition.getTargetName());
                                            globalBackEndTableInfoConfigs.add(globalBackEndTableInfoConfig);
                                        }
                                        globalTableConfig.setBroadcast(globalBackEndTableInfoConfigs);

                                        for (GlobalBackEndTableInfoConfig globalBackEndTableInfoConfig : globalTableConfig.getBroadcast()) {
                                            String targetName = globalBackEndTableInfoConfig.getTargetName();
                                            tableView.getItems().add(targetName);
                                        }
                                        formCell.getChildren().add(parent);
                                        jsonTextArea.setText(Json.encodePrettily(globalTableConfig));

                                        break;
                                    }
                                    case NORMAL: {
                                        NormalTable normalTable = (NormalTable) c;
                                        FXMLLoader loader = UIMain.loader("/singleTable.fxml");
                                        Parent parent = loader.load();

                                        SingleTableVO controller = loader.getController();
                                        controller.getSchemaName().setText(command.getSchema());
                                        controller.getTableName().setText(command.getTable());

                                        Partition dataNode = normalTable.getDataNode();

                                        controller.getTargetName().setText(dataNode.getTargetName());
                                        controller.getPhySchemaName().setText(dataNode.getSchema());
                                        controller.getPhyTableName().setText(dataNode.getTable());

                                        NormalTableConfig normalTableConfig = new NormalTableConfig();
                                        normalTableConfig.setCreateTableSQL(normalTable.getCreateTableSQL());
                                        normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                                                .targetName(dataNode.getTargetName())
                                                .schemaName(dataNode.getSchema())
                                                .tableName(dataNode.getTable())
                                                .build());

                                        formCell.getChildren().add(parent);
                                        jsonTextArea.setText(Json.encodePrettily(normalTableConfig));
                                        break;
                                    }
                                    case CUSTOM:
                                        break;
                                }
                            }
                        });
                        break;
                    case CLUSTER:
                        Optional<ClusterConfig> clusterConfigOptional = infoProvider.getClusterConfigByPath(command.getCluster());
                        clusterConfigOptional.ifPresent(new Consumer<ClusterConfig>() {
                            @Override
                            @SneakyThrows
                            public void accept(ClusterConfig c) {
                                FXMLLoader loader = UIMain.loader("/cluster.fxml");
                                Parent parent = loader.load();

                                ClusterVO controller = loader.getController();
                                controller.getName().setText(c.getName());
                                controller.getType().setText(c.getClusterType());
                                controller.getMasterList().getItems().addAll(c.getMasters());
                                controller.getReplicaList().getItems().addAll(c.getReplicas());
                                formCell.getChildren().add(parent);
                                jsonTextArea.setText(Json.encodePrettily(c));
                            }
                        });
                        break;
                    case DATASOURCE:
                        Optional<DatasourceConfig> datasourceConfigOptional = infoProvider.getDatasourceConfigByPath(command.getDatasource());
                        datasourceConfigOptional.ifPresent(new Consumer<DatasourceConfig>() {
                            @Override
                            @SneakyThrows
                            public void accept(DatasourceConfig c) {
                                FXMLLoader loader = UIMain.loader("/datasource.fxml");
                                Parent parent = loader.load();

                                DatasourceVO controller = loader.getController();
                                controller.getName().setText(c.getName());
                                controller.getUrl().setText(c.getUrl());
                                controller.getDbType().setText(c.getDbType());
                                controller.getType().setText(c.getType());
                                formCell.getChildren().add(parent);
                                jsonTextArea.setText(Json.encodePrettily(c));
                            }
                        });
                        break;
                }
                System.out.println("");
            }
        });
        return cell;
    }
}

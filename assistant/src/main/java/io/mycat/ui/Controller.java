package io.mycat.ui;

import io.mycat.*;
import io.mycat.config.*;
import javafx.collections.ObservableList;
import javafx.event.EventHandler;
import javafx.event.EventType;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.VBox;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.*;

@Data
public class Controller {
    public SplitPane main;
    public TreeView objectTree;
    public TextArea objectText;
    public VBox objectNav;
    private InfoProvider infoProvider;
    private VO currentVO;

    public static Controller INSTANCE;

    public EventHandler navToText = event -> {
        if (currentVO != null) {
            try {
                objectText.setText(currentVO.toJsonConfig());
            } catch (Exception e) {
                MainPaneVO.popAlter(e);
            }

        }
    };
    public EventHandler textToNax = event -> {
        if (currentVO != null) {
            try {
                currentVO.from(objectText.getText());
            } catch (Exception e) {
                MainPaneVO.popAlter(e);
            }
        }

    };


    public Controller() {
        System.out.println();
        INSTANCE = this;
    }

    public void flashRoot() {

        TreeItemCellFactory treeItemCellFactory = new TreeItemCellFactory(this);
        objectTree.setPrefWidth(120);
        objectTree.setMaxWidth(300);
        objectTree.setShowRoot(false);
        objectTree.setCellFactory(treeItemCellFactory);
        TreeItem<ObjectItem> rootViewNode = getRootViewNode(infoProvider);
        objectTree.setRoot(rootViewNode);
        objectNav.setPrefWidth(120);
        objectNav.getChildren().clear();
        TextArea emptyLabel = new TextArea("请选择对象");
        emptyLabel.setPrefHeight(500);
        emptyLabel.setPrefWidth(900);
        objectNav.getChildren().add(emptyLabel);

        objectNav.removeEventHandler(EventType.ROOT, navToText);
        objectNav.addEventHandler(EventType.ROOT, navToText);

        objectText.removeEventHandler(KeyEvent.ANY, textToNax);
        objectText.addEventHandler(KeyEvent.ANY, textToNax);
    }

    public void flashSchemas() {
        ObservableList<TreeItem<ObjectItem>> children = objectTree.getRoot().getChildren();
        for (TreeItem<ObjectItem> child : children) {
            if (child.getValue().getText().equals("schemas")) {
                child.getChildren().clear();
                flashSchemas(infoProvider, child);
                return;
            }
        }

    }

    private void flashSchemas(InfoProvider infoProvider, TreeItem child) {
        for (LogicSchemaConfig schema : infoProvider.schemas()) {

            String schemaName = schema.getSchemaName();
            TreeItem<ObjectItem> schemaItem = new TreeItem(ObjectItem.ofSchema(schemaName));
            child.getChildren().add(schemaItem);


            TreeItem<ObjectItem> shardingTablesItem = new TreeItem(ObjectItem.ofShardingTables(schemaName));
            TreeItem<ObjectItem> globalTablesItem = new TreeItem(ObjectItem.ofGlobalTables(schemaName));
            TreeItem<ObjectItem> singleTablesItem = new TreeItem(ObjectItem.ofSingleTables(schemaName));

            schemaItem.getChildren().add(shardingTablesItem);
            schemaItem.getChildren().add(globalTablesItem);
            schemaItem.getChildren().add(singleTablesItem);

            for (Map.Entry<String, NormalTableConfig> e : schema.getNormalTables().entrySet()) {
                singleTablesItem.getChildren().add(new TreeItem(ObjectItem.ofSingleTable(schemaName, e.getKey())));
            }
            for (Map.Entry<String, GlobalTableConfig> e : schema.getGlobalTables().entrySet()) {
                globalTablesItem.getChildren().add(new TreeItem(ObjectItem.ofGlobalTable(schemaName, e.getKey())));
            }
            for (Map.Entry<String, ShardingTableConfig> e : schema.getShardingTables().entrySet()) {
                shardingTablesItem.getChildren().add(new TreeItem(ObjectItem.ofShardingTable(schemaName, e.getKey())));
            }
            System.out.println();
        }
    }

    public void flashClusterAndDataSource() {
        ObservableList<TreeItem<ObjectItem>> children = objectTree.getRoot().getChildren();
        for (TreeItem<ObjectItem> child : children) {
            if (child.getValue().getText().equals("datasources")) {
                child.getChildren().clear();
                for (DatasourceConfig datasourceConfig : infoProvider.datasources()) {
                    String name = datasourceConfig.getName();
                    child.getChildren().add(new TreeItem(ObjectItem.ofDatasource(name)));
                }
                continue;
            }
            if (child.getValue().getText().equals("clusters")) {
                child.getChildren().clear();
                for (ClusterConfig clusterConfig : infoProvider.clusters()) {
                    String name = clusterConfig.getName();
                    child.getChildren().add(new TreeItem(ObjectItem.ofCluster(name)));
                }
                continue;
            }
        }

    }

    public void edit(DatasourceConfig c) {
        try {
            FXMLLoader loader = UIMain.loader("/datasource.fxml");
            Parent parent = loader.load();
            DatasourceVO datasourceVO = loader.getController();
            datasourceVO.setController(this);
            datasourceVO.setDatasourceConfig(c);
            setCurrentObject(parent, datasourceVO);
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    public void edit(ClusterConfig c) {
        try {
            FXMLLoader loader = UIMain.loader("/cluster.fxml");
            Parent parent = loader.load();

            ClusterVO clusterVO = loader.getController();
            clusterVO.setController(this);
            clusterVO.setClusterConfig(c);
            setCurrentObject(parent, clusterVO);
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    public void edit(LogicTableType logicTableType, String schemaName, String tableName, Object config) {
        try {
            switch (logicTableType) {
                case SHARDING: {
                    FXMLLoader loader = UIMain.loader("/shardingTable.fxml");
                    Parent parent = loader.load();

                    ShardingTableConfigVO shardingTableConfigVO = loader.getController();
                    shardingTableConfigVO.setController(this);
                    shardingTableConfigVO.getSchemaName().setText(schemaName);
                    shardingTableConfigVO.getTableName().setText(tableName);
                    shardingTableConfigVO.setShardingTableConfig((ShardingTableConfig) JsonUtil.clone(config));

                    setCurrentObject(parent, shardingTableConfigVO);
                    break;
                }
                case GLOBAL: {
                    FXMLLoader loader = UIMain.loader("/globalTable.fxml");
                    Parent parent = loader.load();

                    GlobalTableConfigVO globalTableConfigVO = loader.getController();
                    globalTableConfigVO.getSchemaName().setText(schemaName);
                    globalTableConfigVO.getTableName().setText(tableName);
                    globalTableConfigVO.setController(this);
                    globalTableConfigVO.setGlobalTableConfig((GlobalTableConfig) JsonUtil.clone(config));

                    setCurrentObject(parent, globalTableConfigVO);
                    break;
                }
                case NORMAL: {
                    FXMLLoader loader = UIMain.loader("/singleTable.fxml");
                    Parent parent = loader.load();

                    SingleTableVO singleTableVO = loader.getController();
                    singleTableVO.setController(this);
                    singleTableVO.getSchemaName().setText(schemaName);
                    singleTableVO.getTableName().setText(tableName);
                    singleTableVO.setNormalTableConfig((NormalTableConfig) JsonUtil.clone(config));
                    setCurrentObject(parent, singleTableVO);
                    break;
                }
                case CUSTOM:
                    break;
            }
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    private void setCurrentObject(Parent parent, VO vo) {
        Objects.requireNonNull(vo);
        objectNav.getChildren().add(parent);
        objectText.setText(vo.toJsonConfig());
        currentVO = Objects.requireNonNull(vo);
    }

    public static void initPartitionsView(List<Partition> partitions, TableView partitionsView) {
        partitionsView.getColumns().clear();
        partitionsView.getItems().clear();

        partitionsView.setEditable(true);
        partitionsView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);


        TableColumn firstCol = new TableColumn("目标");
        firstCol.setEditable(true);
        firstCol.setCellFactory(TextFieldTableCell.forTableColumn());
        firstCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setTarget(t.getNewValue())
        );
        firstCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("target")
        );

        TableColumn secondCol = new TableColumn("物理分库");
        secondCol.setEditable(true);
        secondCol.setCellFactory(TextFieldTableCell.forTableColumn());
        secondCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setSchema(t.getNewValue())
        );
        secondCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("schema")
        );

        TableColumn thirdCol = new TableColumn("物理分表");
        thirdCol.setEditable(true);
        thirdCol.setCellFactory(TextFieldTableCell.forTableColumn());
        thirdCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setTable(t.getNewValue())
        );
        thirdCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("table")
        );


        TableColumn fourthCol = new TableColumn("物理分库下标");
        fourthCol.setEditable(true);
        fourthCol.setCellFactory(TextFieldTableCell.forTableColumn());
        fourthCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setDbIndex((t.getNewValue()))
        );
        fourthCol.setCellValueFactory(
                new PropertyValueFactory<PartitionEntry, String>("dbIndex")
        );

        TableColumn fifthCol = new TableColumn("物理分表下标");
        fifthCol.setEditable(true);
        fifthCol.setCellFactory(TextFieldTableCell.forTableColumn());
        fifthCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setTableIndex((t.getNewValue()))
        );
        fifthCol.setCellValueFactory(
                new PropertyValueFactory<PartitionEntry, Integer>("tableIndex")
        );

        TableColumn sixCol = new TableColumn("总物理分表下标");
        sixCol.setEditable(true);
        sixCol.setCellFactory(TextFieldTableCell.forTableColumn());
        sixCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setGlobalIndex((t.getNewValue()))
        );
        sixCol.setCellValueFactory(
                new PropertyValueFactory<PartitionEntry, Integer>("globalIndex")
        );

        partitionsView.getColumns().addAll(firstCol, secondCol, thirdCol, fourthCol, fifthCol, sixCol);

        for (Partition backend : partitions) {
            partitionsView.getItems().add(PartitionEntry.of(backend.getIndex(),backend.getDbIndex(),backend.getTableIndex(),backend.getTargetName(),backend.getSchema(),backend.getTable()));
        }

        partitionsView.refresh();
    }

    public void edit(LogicSchemaConfig r) {
        FXMLLoader loader = UIMain.loader("/schema.fxml");
        Parent parent = null;
        try {
            parent = loader.load();
            SchemaConfigVO schemaConfigVO = loader.getController();
            schemaConfigVO.setController(this);
            schemaConfigVO.setLogicSchemaConfig(r);
            setCurrentObject(parent, schemaConfigVO);
        } catch (IOException e) {
            MainPaneVO.popAlter(e);
        }
    }

    public TreeItem<ObjectItem> getRootViewNode(InfoProvider infoProvider) {
        TreeItem<ObjectItem> rootItem = new TreeItem(ObjectItem.builder().id("root").text("root").object("root").build());


        TreeItem<ObjectItem> schemaItems = new TreeItem<>(ObjectItem.builder().id("schemas").text("schemas").object("schemas").build());
        TreeItem<ObjectItem> clusterItems = new TreeItem<>(ObjectItem.builder().id("clusters").text("clusters").object("clusters").build());
        TreeItem<ObjectItem> datasourceItems = new TreeItem<>(ObjectItem.builder().id("datasources").text("datasources").object("datasources").build());

        rootItem.getChildren().add(schemaItems);
        rootItem.getChildren().add(clusterItems);
        rootItem.getChildren().add(datasourceItems);

        flashSchemas(infoProvider, schemaItems);

        for (ClusterConfig cluster : infoProvider.clusters()) {
            String name = cluster.getName();
            clusterItems.getChildren().add(new TreeItem(ObjectItem.ofCluster(name)));
        }

        for (DatasourceConfig datasourceConfig : infoProvider.datasources()) {
            String name = datasourceConfig.getName();
            datasourceItems.getChildren().add(new TreeItem(ObjectItem.ofDatasource(name)));
        }

        return rootItem;
    }

    public void clearObjectInfo() {
        objectText.setText("");
        objectNav.getChildren().clear();
    }

    public void addShardingTable(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/shardingTable.fxml");
            Parent parent = loader.load();
            ShardingTableConfigVO controller = loader.getController();
            controller.setController(this);
            controller.getSchemaName().setText(schema);
            controller.setShardingTableConfig(new ShardingTableConfig());
            setCurrentObject(parent, controller);
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    public void addGlobalTableConfig(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/globalTable.fxml");
            Parent parent = loader.load();
            GlobalTableConfigVO controller = loader.getController();
            controller.setController(this);
            controller.getSchemaName().setText(schema);
            controller.getTargets().setEditable(true);
            setCurrentObject(parent, controller);
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    public void addNormalTableConfig(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/singleTable.fxml");
            Parent parent = loader.load();
            SingleTableVO singleTableVO = loader.getController();
            singleTableVO.setController(this);
            singleTableVO.getSchemaName().setText(schema);

            NormalTableConfig normalTableConfig = new NormalTableConfig();
            singleTableVO.setNormalTableConfig(normalTableConfig);
            setCurrentObject(parent, singleTableVO);
        } catch (Exception e) {
            MainPaneVO.popAlter(e);
        }
    }

    public void save(String schemaName, String tableName, ShardingTableConfig config) {
        infoProvider.saveShardingTable(schemaName, tableName, config);
        flashSchemas();
    }

    public void setInfoProvider(InfoProvider infoProvider) {
        this.infoProvider = infoProvider;
    }

    public void save(String schemaName, String tableName, NormalTableConfig config) {
        infoProvider.saveSingleTable(schemaName, tableName, config);
        flashSchemas();

    }

    @SneakyThrows
    public void saveDatasource(DatasourceConfig config) {
        infoProvider.saveDatasource(config);

    }

    @SneakyThrows
    public void saveCluster(ClusterConfig config) {
        infoProvider.saveCluster(config);
    }

    public void save(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        infoProvider.saveGlobalTable(schemaName, tableName, globalTableConfig);
        flashSchemas();
    }

    public void saveSchema(LogicSchemaConfig logicSchemaConfig) {
        infoProvider.saveSchema(logicSchemaConfig);
        flashSchemas();
    }
}

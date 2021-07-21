package io.mycat.ui;

import io.mycat.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.*;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Data
public class Controller {
    public AnchorPane mainPane;
    public MenuBar menu;
    public SplitPane main;
    public TreeView objectTree;
    public TextArea objectText;
    public VBox objectNav;
    private InfoProvider infoProvider;

    public Controller() {
        System.out.println();
    }

    public void flashRoot(InfoProvider infoProvider) {
        TreeItemCellFactory treeItemCellFactory = new TreeItemCellFactory(this);
        objectTree.setShowRoot(false);
        objectTree.setCellFactory(treeItemCellFactory);
        TreeItem<String> rootViewNode = getRootViewNode(infoProvider);
        objectTree.setRoot(rootViewNode);
    }

    public void flashSchemas(InfoProvider infoProvider) {
        ObservableList<TreeItem> children = objectTree.getRoot().getChildren();
        for (TreeItem child : children) {
            if (child.getValue().equals("schemas")) {
                child.getChildren().clear();
                flashSchemas(infoProvider, child);
                return;
            }
        }

    }

    private void flashSchemas(InfoProvider infoProvider, TreeItem child) {
        for (SchemaHandler schema : infoProvider.schemas()) {

            TreeItem<String> schemaItem = new TreeItem(schema.getName());
            child.getChildren().add(schemaItem);
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
    }

    public void flashDataSource(InfoProvider infoProvider) {
        ObservableList<TreeItem> children = objectTree.getRoot().getChildren();
        for (TreeItem child : children) {
            if (child.getValue().equals("datasources")) {
                child.getChildren().clear();
                for (DatasourceConfig datasourceConfig : infoProvider.datasources()) {
                    child.getChildren().add(new TreeItem(datasourceConfig.getName()));
                }
                return;
            }
        }

    }

    public void edit(DatasourceConfig c){
        try {
        FXMLLoader loader = UIMain.loader("/datasource.fxml");
        Parent parent = loader.load();

        DatasourceVO controller = loader.getController();
        controller.getName().setText(c.getName());
        controller.getUrl().setText(c.getUrl());
        controller.getDbType().setText(c.getDbType());
        controller.getType().setText(c.getType());
        objectNav.getChildren().add(parent);
        objectText.setText(Json.encodePrettily(c));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void edit(ClusterConfig c){
        try {
        FXMLLoader loader = UIMain.loader("/cluster.fxml");
        Parent parent = loader.load();

        ClusterVO controller = loader.getController();
        controller.getName().setText(c.getName());
        controller.getType().setText(c.getClusterType());
        controller.getMasterList().getItems().addAll(c.getMasters());
        controller.getReplicaList().getItems().addAll(c.getReplicas());
        objectNav.getChildren().add(parent);
        objectText.setText(Json.encodePrettily(c));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void edit(TableHandler c){
        try {
            switch (c.getType()) {
                case SHARDING: {
                    ShardingTable shardingTable = (ShardingTable) c;
                    FXMLLoader loader = UIMain.loader("/shardingTable.fxml");
                    Parent parent = loader.load();

                    ShardingTableConfigVO shardingTableConfigVO = loader.getController();
                    shardingTableConfigVO.setController(this);
                    shardingTableConfigVO.getSchemaName().setText(c.getSchemaName());
                    shardingTableConfigVO.getTableName().setText(c.getTableName());

                    ShardingTableConfig shardingTableConfig = shardingTable.getTableConfigConfig();
                    shardingTableConfigVO.getShardingInfo().setText(Json.encodePrettily(shardingTableConfig.getFunction()));
                    shardingTableConfigVO.getCreateTableSQL().setText(shardingTable.getCreateTableSQL());

                    TableView partitionsView = shardingTableConfigVO.getPartitionsView();
                    initPartitionsView(shardingTable.getBackends(), partitionsView);
                    objectNav.getChildren().add(parent);
                    objectText.setText(Json.encodePrettily(shardingTableConfig));
                    break;
                }
                case GLOBAL: {
                    GlobalTable globalTable = (GlobalTable) c;
                    FXMLLoader loader = UIMain.loader("/globalTable.fxml");
                    Parent parent = loader.load();

                    GlobalTableConfigVO controller = loader.getController();
                    controller.setController(this);
                    controller.getSchemaName().setText(c.getSchemaName());
                    controller.getTableName().setText(c.getTableName());

                    ListView tableView = controller.getTargets();

                    tableView.setCellFactory(TextFieldListCell.forListView());
                    tableView.setEditable(true);

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
                    objectNav.getChildren().add(parent);
                    objectText.setText(Json.encodePrettily(globalTableConfig));


                    break;
                }
                case NORMAL: {
                    NormalTable normalTable = (NormalTable) c;
                    FXMLLoader loader = UIMain.loader("/singleTable.fxml");
                    Parent parent = loader.load();

                    SingleTableVO controller = loader.getController();
                    controller.setController(this);

                    controller.getSchemaName().setText(c.getSchemaName());
                    controller.getTableName().setText(c.getTableName());

                    Partition dataNode = normalTable.getDataNode();

                    controller.getTargetName().setText(dataNode.getTargetName());
                    controller.getPhySchemaName().setText(dataNode.getSchema());
                    controller.getPhyTableName().setText(dataNode.getTable());
                    controller.getCreateTableSQL().setText(normalTable.getCreateTableSQL());

                    NormalTableConfig normalTableConfig = new NormalTableConfig();
                    normalTableConfig.setCreateTableSQL(normalTable.getCreateTableSQL());
                    normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                            .targetName(dataNode.getTargetName())
                            .schemaName(dataNode.getSchema())
                            .tableName(dataNode.getTable())
                            .build());
                    objectNav.getChildren().add(parent);
                    objectText.setText(Json.encodePrettily(normalTableConfig));

                    break;
                }
                case CUSTOM:
                    break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void initPartitionsView(List<Partition>  partitions,TableView partitionsView) {
        partitionsView.getColumns().clear();
        partitionsView.getItems().clear();

        partitionsView.setEditable(true);
        partitionsView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        int index = 0;
        for (Partition backend : partitions) {
            partitionsView.getItems().add(PartitionEntry.of(index++,backend));
        }

        TableColumn firstCol = new TableColumn("目标");
        firstCol.setEditable(true);
        firstCol.setCellFactory(TextFieldTableCell.forTableColumn());
        firstCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t. getTablePosition().getRow()).setTarget(t.getNewValue())
        );
        firstCol.setOnEditStart(new EventHandler<TableColumn.CellEditEvent>() {
            @Override
            public void handle(TableColumn.CellEditEvent event) {
                System.out.println();
            }
        });

        TableColumn secondCol = new TableColumn("物理分库");
        secondCol.setEditable(true);
        secondCol.setCellFactory(TextFieldTableCell.forTableColumn());
        secondCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t. getTablePosition().getRow()).setSchema(t.getNewValue())
        );

        TableColumn thirdCol = new TableColumn("物理分表");
        thirdCol.setEditable(true);
        thirdCol.setCellFactory(TextFieldTableCell.forTableColumn());
        thirdCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t. getTablePosition().getRow()).setTable(t.getNewValue())
        );

        secondCol.setEditable(true);
        thirdCol.setEditable(true);


        firstCol.setOnEditStart(new EventHandler<TableColumn.CellEditEvent>() {
            @Override
            public void handle(TableColumn.CellEditEvent event) {
                System.out.println();
            }
        });
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

    }

    public void edit(LogicSchemaConfig r) {
        FXMLLoader loader = UIMain.loader("/schema.fxml");
        Parent parent = null;
        try {
            parent = loader.load();
            SchemaConfigVO schemaConfigVO = loader.getController();
            schemaConfigVO.getSchemaName().setText(r.getSchemaName());
            schemaConfigVO.getDefaultTargetName().setText(r.getTargetName());
            objectNav.getChildren().add(parent);
            objectText.setText(Json.encodePrettily(r));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TreeItem<String> getRootViewNode(InfoProvider infoProvider) {
        TreeItem<String> rootItem = new TreeItem("root");


        TreeItem<String> schemaItems = new TreeItem<>("schemas");
        TreeItem<String> clusterItems = new TreeItem<>("clusters");
        TreeItem<String> datasourceItems = new TreeItem<>("datasources");

        rootItem.getChildren().add(schemaItems);
        rootItem.getChildren().add(clusterItems);
        rootItem.getChildren().add(datasourceItems);

        flashSchemas(infoProvider, schemaItems);

        for (ClusterConfig cluster : infoProvider.clusters()) {
            clusterItems.getChildren().add(new TreeItem(cluster.getName()));
        }

        for (DatasourceConfig datasourceConfig : infoProvider.datasources()) {
            datasourceItems.getChildren().add(new TreeItem(datasourceConfig.getName()));
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
            controller.getSchemaName().setText(schema);
            objectNav.getChildren().add(parent);
            objectText.setText(Json.encodePrettily(new ShardingTableConfig()));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void addGlobalTableConfig(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/globalTable.fxml");
            Parent parent = loader.load();
            GlobalTableConfigVO controller = loader.getController();
            controller.getSchemaName().setText(schema);
            controller.getTargets().setCellFactory(TextFieldListCell.forListView());
            controller.getTargets().setEditable(true);
            objectNav.getChildren().add(parent);
            objectText.setText(Json.encodePrettily(new GlobalTableConfig()));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void addNormalTableConfig(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/singleTable.fxml");
            Parent parent = loader.load();
            SingleTableVO controller = loader.getController();
            controller.getSchemaName().setText(schema);

            NormalTableConfig normalTableConfig = new NormalTableConfig();
            objectNav.getChildren().add(parent);
            objectText.setText(Json.encodePrettily(normalTableConfig));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void save(String schemaName, String tableName,String createSQL, ShardingFuntion shardingFuntion, List<Partition> partitions) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.addShardingTable(schemaName,tableName,ShardingTableConfig.builder()
                .createTableSQL(createSQL).function(shardingFuntion).build(), metadataManager.getPrototype(),partitions);
        flashSchemas(infoProvider);
    }

    public void setInfoProvider(InfoProvider infoProvider) {
        this.infoProvider = infoProvider;
    }

    public void save(String schemaName, String tableName, String sql, String targetName, String phySchemaName, String phyTableName) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.addNormalTable(schemaName,tableName,NormalTableConfig.create(phySchemaName,phyTableName,sql,targetName),metadataManager.getPrototype());
        flashSchemas(infoProvider);
    }

    public void save(String schemaName, String tableName, String sql, List<String> targets) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<Partition> list = new ArrayList<>();
        List<GlobalBackEndTableInfoConfig> configList = new ArrayList<>();
        for (String target : targets) {
            list.add( new BackendTableInfo(target, schemaName, tableName));
            configList.add(GlobalBackEndTableInfoConfig.builder().targetName(target).build());
        }

        metadataManager.addGlobalTable(schemaName,tableName,GlobalTableConfig.builder().createTableSQL(sql).broadcast(configList).build(),metadataManager.getPrototype(),list);
        flashSchemas(infoProvider);
    }
}

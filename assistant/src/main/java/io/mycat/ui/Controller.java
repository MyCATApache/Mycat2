package io.mycat.ui;

import io.mycat.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.*;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.Event;
import javafx.event.EventHandler;
import javafx.event.EventType;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.input.InputEvent;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;
import static io.mycat.LogicTableType.GLOBAL;
import static io.mycat.LogicTableType.SHARDING;

@Data
public class Controller {
    public SplitPane main;
    public TreeView objectTree;
    public TextArea objectText;
    public VBox objectNav;
    private InfoProvider infoProvider;
    private VO currentVO;

    private EventHandler<KeyEvent> navToText = event -> {
        if (currentVO != null) {
            try {
                objectText.setText(currentVO.toJsonConfig());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    };
    private EventHandler<KeyEvent> textToNax = event -> {
        if (currentVO != null) {
            try {
                currentVO.from(objectText.getText());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    };


    public Controller() {
        System.out.println();
    }

    public void flashRoot() {

        TreeItemCellFactory treeItemCellFactory = new TreeItemCellFactory(this);
        objectTree.setPrefWidth(120);
        objectTree.setMaxWidth(300);
        objectTree.setShowRoot(false);
        objectTree.setCellFactory(treeItemCellFactory);
        TreeItem<String> rootViewNode = getRootViewNode(infoProvider);
        objectTree.setRoot(rootViewNode);
        objectNav.setPrefWidth(120);
        objectNav.getChildren().clear();
        TextArea emptyLabel = new TextArea("请选择对象");
        objectNav.getChildren().add(emptyLabel);

        objectNav.removeEventHandler(KeyEvent.ANY, navToText);
        objectNav.addEventHandler(KeyEvent.ANY, navToText);
        objectNav.removeEventHandler(KeyEvent.ANY, textToNax);
        objectText.addEventHandler(KeyEvent.ANY, textToNax);
    }

    public void flashSchemas() {
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

    public void flashDataSource() {
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

    public void edit(DatasourceConfig c) {
        try {
            FXMLLoader loader = UIMain.loader("/datasource.fxml");
            Parent parent = loader.load();
            DatasourceVO datasourceVO = loader.getController();
            datasourceVO.setController(this);
            datasourceVO.setDatasourceConfig(c);
            setCurrentObject(parent, datasourceVO);
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }

    public void edit(TableHandler c) {
        try {
            switch (c.getType()) {
                case SHARDING: {
                    ShardingTable shardingTable = (ShardingTable) c;
                    FXMLLoader loader = UIMain.loader("/shardingTable.fxml");
                    Parent parent = loader.load();

                    ShardingTableConfigVO shardingTableConfigVO = loader.getController();
                    shardingTableConfigVO.setController(this);
                    shardingTableConfigVO.getSchemaName().setText(shardingTable.getSchemaName());
                    shardingTableConfigVO.getTableName().setText(shardingTable.getTableName());
                    shardingTableConfigVO.setShardingTableConfig(JsonUtil.clone(shardingTable.getTableConfig()));

                    setCurrentObject(parent, shardingTableConfigVO);
                    break;
                }
                case GLOBAL: {
                    GlobalTable globalTable = (GlobalTable) c;
                    FXMLLoader loader = UIMain.loader("/globalTable.fxml");
                    Parent parent = loader.load();

                    GlobalTableConfigVO globalTableConfigVO = loader.getController();
                    globalTableConfigVO.setController(this);
                    globalTableConfigVO.setGlobalTableConfig(JsonUtil.clone(globalTable.getTableConfig()));

                    setCurrentObject(parent, globalTableConfigVO);
                    break;
                }
                case NORMAL: {
                    NormalTable normalTable = (NormalTable) c;
                    FXMLLoader loader = UIMain.loader("/singleTable.fxml");
                    Parent parent = loader.load();

                    SingleTableVO singleTableVO = loader.getController();
                    singleTableVO.setController(this);
                    singleTableVO.getSchemaName().setText(normalTable.getSchemaName());
                    singleTableVO.getTableName().setText(normalTable.getTableName());
                    singleTableVO.setNormalTableConfig(JsonUtil.clone(normalTable.getTableConfig()));
                    setCurrentObject(parent, singleTableVO);
                    break;
                }
                case CUSTOM:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setCurrentObject(Parent parent, VO vo) {
        objectNav.getChildren().add(parent);
        objectText.setText(vo.toJsonConfig());
        currentVO = vo;
    }

    public static void initPartitionsView(List<Partition> partitions, TableView partitionsView) {
        partitionsView.getColumns().clear();
        partitionsView.getItems().clear();

        partitionsView.setEditable(true);
        partitionsView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        int index = 0;
        for (Partition backend : partitions) {
            partitionsView.getItems().add(PartitionEntry.of(index++, backend));
        }

        TableColumn firstCol = new TableColumn("目标");
        firstCol.setEditable(true);
        firstCol.setCellFactory(TextFieldTableCell.forTableColumn());
        firstCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setTarget(t.getNewValue())
        );
        firstCol.setOnEditStart(new EventHandler<TableColumn.CellEditEvent>() {
            @Override
            public void handle(TableColumn.CellEditEvent event) {
                System.out.println();
            }
        });
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
                        t.getTablePosition().getRow()).setDbIndex(Integer.parseInt(t.getNewValue()))
        );
        thirdCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("dbIndex")
        );

        TableColumn fifthCol = new TableColumn("物理分表下标");
        fifthCol.setEditable(true);
        fifthCol.setCellFactory(TextFieldTableCell.forTableColumn());
        fifthCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setTableIndex(Integer.parseInt(t.getNewValue()))
        );
        fifthCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("tableIndex")
        );

        TableColumn sixCol = new TableColumn("总物理分表下标");
        sixCol.setEditable(true);
        sixCol.setCellFactory(TextFieldTableCell.forTableColumn());
        sixCol.setOnEditCommit(
                (EventHandler<TableColumn.CellEditEvent<PartitionEntry, String>>) t -> t.getTableView().getItems().get(
                        t.getTablePosition().getRow()).setGlobalIndex(Integer.parseInt(t.getNewValue()))
        );
        fifthCol.setCellValueFactory(
                new PropertyValueFactory<Partition, String>("globalIndex")
        );

        partitionsView.getColumns().addAll(firstCol, secondCol, thirdCol,fourthCol,fifthCol,sixCol);

    }

    public void edit(LogicSchemaConfig r) {
        FXMLLoader loader = UIMain.loader("/schema.fxml");
        Parent parent = null;
        try {
            parent = loader.load();
            SchemaConfigVO schemaConfigVO = loader.getController();
            schemaConfigVO.setLogicSchemaConfig(r);
            setCurrentObject(parent, schemaConfigVO);
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
            controller.setController(this);
            controller.getSchemaName().setText(schema);
            controller.setShardingTableConfig(new ShardingTableConfig());
            setCurrentObject(parent, controller);
        } catch (Exception e) {
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
            setCurrentObject(parent, controller);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addNormalTableConfig(String schema) {
        try {
            FXMLLoader loader = UIMain.loader("/singleTable.fxml");
            Parent parent = loader.load();
            SingleTableVO singleTableVO = loader.getController();
            singleTableVO.getSchemaName().setText(schema);

            NormalTableConfig normalTableConfig = new NormalTableConfig();
            singleTableVO.setNormalTableConfig(normalTableConfig);
            setCurrentObject(parent, singleTableVO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void save(String schemaName, String tableName, ShardingTableConfig config) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.addShardingTable(schemaName, tableName, config, metadataManager.getPrototype(), MetadataManager.getBackendTableInfos(config.getPartition()), Collections.emptyList());
        flashSchemas();
    }

    public void setInfoProvider(InfoProvider infoProvider) {
        this.infoProvider = infoProvider;
    }

    public void save(String schemaName, String tableName, NormalTableConfig config) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.addNormalTable(schemaName, tableName, config, metadataManager.getPrototype());
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
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<Partition> list = new ArrayList<>();
        List<GlobalBackEndTableInfoConfig> configList = globalTableConfig.getBroadcast();

        for (GlobalBackEndTableInfoConfig target : configList) {
            list.add(new BackendTableInfo(target.getTargetName(), schemaName, tableName));
        }

        metadataManager.addGlobalTable(schemaName, tableName, globalTableConfig, metadataManager.getPrototype(), list);
        flashSchemas();
    }
}

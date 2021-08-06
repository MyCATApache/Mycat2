package io.mycat.ui;

import io.mycat.*;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFunction;
import io.mycat.config.ShardingTableConfig;
import io.mycat.util.StringUtil;
import io.vertx.core.json.Json;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ListView;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.mycat.ui.Controller.initPartitionsView;

@Data
public class ShardingTableConfigVO implements VO {

    @FXML
    public TextField schemaName;

    @FXML
    public TextField tableName;

    @FXML
    public TextArea shardingInfo;

    @FXML
    public TableView<PartitionEntry> partitionsView;

    public Controller controller;

    @FXML
    public TextArea createTableSQL;
    @FXML
    public ListView indexTableList;


    private String selectIndex;

    public ShardingTableConfig shardingTableConfig;

    public File testFile;

    public  IndexShardingTableVO currentIndexShardingTableController;

    public ShardingTableConfigVO() {

    }

    public void setShardingTableConfig(ShardingTableConfig shardingTableConfig) {
        this.shardingTableConfig = shardingTableConfig;
        flash();
    }

    public void save() {
        System.out.println();
        String schemaName = this.schemaName.getText();
        String tableName = this.tableName.getText();
        ShardingTableConfig shardingTableConfig = getShardingTableConfig();
        controller.save(schemaName, tableName, shardingTableConfig);
        controller.flashSchemas();
    }

    public void flash() {
        this.getShardingInfo().setText(Json.encodePrettily(shardingTableConfig.getFunction()));
        this.getCreateTableSQL().setText(shardingTableConfig.getCreateTableSQL());

        TableView partitionsView = this.getPartitionsView();
        initPartitionsView(MetadataManager.getBackendTableInfos(shardingTableConfig.getPartition()), partitionsView);

        indexTableList.getItems().clear();
        for (Map.Entry<String, ShardingTableConfig> e : shardingTableConfig.getShardingIndexTables().entrySet()) {
//            ShardingTableConfig indexTable = e.getValue();
            indexTableList.getItems().add(e.getKey().replace(getTableName().getText() + "_", ""));
//            IndexShardingTableVO indexShardingTableVO = new IndexShardingTableVO();
//            indexShardingTableVO.getSchemaName().setText(getSchemaName().getText());
//            indexShardingTableVO.getTableName().setText(e.getKey());
//            indexShardingTableVO.getIndexName().setText(e.getKey().replace(getTableName().getText(), ""));
//            indexShardingTableVO.getCreateTableSQL().setText(indexTable.getCreateTableSQL());
//
//            TableView subPartitionsView = indexShardingTableVO.getPartitionsView();
//            initPartitionsView(MetadataManager.getBackendTableInfos(indexTable.getPartition()), subPartitionsView);
//            indexTables.add(indexShardingTableVO);
        }
    }

    @NotNull
    public ShardingTableConfig getShardingTableConfig() {
        String sql = this.createTableSQL.getText();

        String shardingInfoText = this.shardingInfo.getText();

        List<List> partitions = new ArrayList<>();
        for (PartitionEntry item : partitionsView.getItems()) {
            Partition partition = item.toPartition();
            String targetName = partition.getTargetName();
            String schema = partition.getSchema();
            String table = partition.getTable();
            Integer dbIndex = partition.getDbIndex();
            Integer tableIndex = partition.getTableIndex();
            Integer index = partition.getIndex();
            partitions.add(Arrays.asList(targetName, schema, table, dbIndex, tableIndex, index));
        }

        ShardingFunction shardingFuntion = Json.decodeValue(shardingInfoText, ShardingFunction.class);

        shardingTableConfig.setCreateTableSQL(sql);
        shardingTableConfig.setFunction(shardingFuntion);
        shardingTableConfig.setPartition(ShardingBackEndTableInfoConfig.builder().data(partitions).build());
        return shardingTableConfig;
    }

    public static void inputPartitionsWithTestFile(TableView<PartitionEntry> view, File testFile) {
        try {
            File file;
            if (testFile == null) {
                FileChooser fileChooser = new FileChooser();
                fileChooser.setTitle("请选择分区csv文件");
                Stage stage = new Stage();
                file = fileChooser.showOpenDialog(stage);
            } else {
                file = testFile;
                testFile = null;
            }

            inputPartitions(view, file);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void inputPartitions(File file) throws IOException {
        inputPartitions(getPartitionsView(), file);
    }

    private static void inputPartitions(TableView<PartitionEntry> view, File file) throws IOException {
        List<Partition> partitions = new LinkedList<>();
        try (CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.RFC4180);) {
            List<String> headerNames = parser.getHeaderNames();
            int size = 6;
            for (CSVRecord csvRecord : parser) {
                partitions.add(new IndexBackendTableInfo(
                        csvRecord.get(size - 6),
                        csvRecord.get(size - 5),
                        csvRecord.get(size - 4),
                        parseInt(csvRecord.get(size - 3)),
                        parseInt(csvRecord.get(size - 2)),
                        parseInt(csvRecord.get(size - 1))));
            }
            initPartitionsView(partitions, view);
        }
    }

    static int parseInt(String text) {
        if (StringUtil.isEmpty(text)) {
            return 0;
        }
        return Integer.parseInt(text);
    }

    @SneakyThrows
    public void inputPartitions(ActionEvent actionEvent) {
        inputPartitionsWithTestFile(getPartitionsView(), testFile);
    }

//    public void flashIndexTableList() {
//        indexTables = null;
//        if (indexTables == null) {
//            for (ShardingIndexTable indexTable : shardingTable.getIndexTables()) {
//                ShardingTableConfig tableConfig = indexTable.getTableConfig();
//
//                IndexShardingTableVO indexShardingTableVO = new IndexShardingTableVO();
//                indexShardingTableVO.getSchemaName().setText(indexTable.getSchemaName());
//                indexShardingTableVO.getTableName().setText(indexTable.getTableName());
//                indexShardingTableVO.getIndexName().setText(indexTable.getIndexName());
//                indexShardingTableVO.getShardingInfo().setText(Json.encodePrettily(tableConfig.getFunction()));
//                initPartitionsView(MetadataManager.getBackendTableInfos(tableConfig.getPartition()), this.partitionsView);
//            }
//
//        }
//        for (IndexShardingTableVO indexTable : indexTables) {
//            indexTableList.getItems().add(indexTable.getIndexName());
//        }
//
//    }


    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getShardingTableConfig());
    }

//    public void setShardingTable(ShardingTable shardingTable) {
//        this.shardingTable = shardingTable;
//
//        this.getSchemaName().setText(shardingTable.getSchemaName());
//        this.getTableName().setText(shardingTable.getTableName());
//
//        ShardingTableConfig shardingTableConfig = shardingTable.getTableConfig();
//        this.getShardingInfo().setText(Json.encodePrettily(shardingTableConfig.getFunction()));
//        this.getCreateTableSQL().setText(shardingTable.getCreateTableSQL());
//
//        TableView partitionsView = this.getPartitionsView();
//        initPartitionsView(shardingTable.getBackends(), partitionsView);
//
//        indexTables = new ArrayList<>();
//        for (Map.Entry<String, ShardingTableConfig> e : shardingTableConfig.getShardingIndexTables().entrySet()) {
//            ShardingTableConfig indexTable = e.getValue();
//            IndexShardingTableVO indexShardingTableVO = new IndexShardingTableVO();
//            indexShardingTableVO.getSchemaName().setText(shardingTable.getSchemaName());
//            indexShardingTableVO.getTableName().setText(e.getKey());
//            indexShardingTableVO.getIndexName().setText(e.getKey().replace(shardingTable.getTableName(), ""));
//            indexShardingTableVO.getCreateTableSQL().setText(indexTable.getCreateTableSQL());
//
//            TableView subPartitionsView = indexShardingTableVO.getPartitionsView();
//            initPartitionsView(MetadataManager.getBackendTableInfos(indexTable.getPartition()), subPartitionsView);
//            indexTables.add(indexShardingTableVO);
//        }
//
//        flashIndexTableList();
//
//        indexTableList.getSelectionModel().selectedItemProperty().addListener((ChangeListener<String>) (observableValue, s, t1) -> selectIndex = t1);
//    }

    public void deleteIndexTable(ActionEvent actionEvent) {
        String schemaName = getSchemaName().getText();
        String tableName = getTableName().getText();
        shardingTableConfig.getShardingIndexTables().remove(tableName + "_" + indexTableList.getSelectionModel().getSelectedItem());
        flash();

    }

    public void addIndexTable(ActionEvent actionEvent) {
        try {
            FXMLLoader loader = UIMain.loader("/indexShardingTable.fxml");
            Parent parent = loader.load();
            this. currentIndexShardingTableController = loader.getController();
            currentIndexShardingTableController.setController(this.controller);
            currentIndexShardingTableController.getLogicalSchemaName().setText(getSchemaName().getText());
            currentIndexShardingTableController.getLogicalTableName().setText(Objects.requireNonNull(getTableName().getText()));
            currentIndexShardingTableController.setShardingTableConfigVO(this);

            Stage stage = new Stage();
            Scene scene = SceneUtil.createScene(()->new Scene(parent));
            stage.setScene(scene);
            currentIndexShardingTableController.setStage(stage);
            stage.showAndWait();
            SceneUtil.close(scene);
            flash();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void from(String text) {
        setShardingTableConfig(Json.decodeValue(text, shardingTableConfig.getClass()));
    }
}

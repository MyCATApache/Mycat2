package io.mycat.ui;

import io.mycat.BackendTableInfo;
import io.mycat.LogicTableType;
import io.mycat.Partition;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.GlobalTableConfig;
import io.mycat.config.ShardingFuntion;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
public class ShardingTableConfigVO implements VO{

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

    public void save() {
        System.out.println();

        String schemaName = this.schemaName.getText();
        String tableName = this.tableName.getText();
        String sql = this.createTableSQL.getText();

        String shardingInfoText = this.shardingInfo.getText();

        List<Partition> partitions = new ArrayList<>();
        for (PartitionEntry item : partitionsView.getItems()) {
            Partition partition = item.toPartition();
            partitions.add(partition);
        }

        controller.save(schemaName,tableName,sql, Json.decodeValue(shardingInfoText, ShardingFuntion.class),partitions);

    }

    public void inputPartitions(ActionEvent actionEvent) {
        try {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("请选择分区csv文件");
            Stage stage = new Stage();
            File file = fileChooser.showOpenDialog(stage);

            List<Partition> partitions = new LinkedList<>();
            try(CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.RFC4180);){
                List<String> headerNames = parser.getHeaderNames();
                int size = 3;
                for (CSVRecord csvRecord : parser) {
                    partitions.add( new BackendTableInfo(csvRecord.get(size-3),csvRecord.get(size-2),csvRecord.get(size-1)));
                }
                Controller.initPartitionsView(partitions,this.partitionsView);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void indexTableList(ActionEvent actionEvent) {
        try {
            FXMLLoader loader = UIMain.loader("/indexShardingTableList.fxml");
            Parent parent = loader.load();
            IndexShardingTableListVO controller = loader.getController();
            controller.setController(getController());
            String schemaName = getSchemaName().getText();
            String tableName = getTableName().getText();

            controller.getSchemaName().setText(schemaName);
            controller.getTableName().setText(tableName);

            ObservableList<String> items = controller.getIndexTableList().getItems();
            items.clear();
            this.getController().getInfoProvider().getTableConfigByName(schemaName,tableName)
                    .ifPresent(c->{
                        if(c.getType() == LogicTableType.SHARDING){
                            ShardingTable shardingTable = (ShardingTable) c;
                            for (ShardingIndexTable indexTable : shardingTable.getIndexTables()) {
                                items.add(indexTable.getIndexName());
                            }

                        }
                    });
            Stage stage = new Stage();
            Scene scene = new Scene(parent, 400, 200);
            stage.setScene(scene);
            stage.show();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String toJsonConfig() {
        return null;
    }
}

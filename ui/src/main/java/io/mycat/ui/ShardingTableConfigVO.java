package io.mycat.ui;

import io.mycat.BackendTableInfo;
import io.mycat.Partition;
import io.mycat.config.LogicSchemaConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.config.SharingFuntionRootConfig;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
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
public class ShardingTableConfigVO {

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
}

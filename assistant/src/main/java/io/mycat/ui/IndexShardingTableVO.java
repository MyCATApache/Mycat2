package io.mycat.ui;

import io.mycat.LogicTableType;
import io.mycat.Partition;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.config.ShardingTableConfig;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.Stage;
import lombok.Data;

import java.util.*;
import java.util.stream.Collectors;

@Data
public class IndexShardingTableVO implements VO{
    public Label schemaName;
    public Label tableName;
    public TextField indexName;
    public Controller controller;

    @FXML
    public TextArea shardingInfo;

    @FXML
    public TableView<PartitionEntry> partitionsView;

    @FXML
    public TextArea createTableSQL;
    private ShardingTableConfigVO shardingTableConfigVO;

    public void inputPartitions(ActionEvent actionEvent) {

    }

    public ShardingTableConfig toShardingTableConfig(){
        ShardingTableConfig shardingTableConfig = new ShardingTableConfig();
        shardingTableConfig.setShardingIndexTables(Collections.emptyMap());


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
            partitions.add(Arrays.asList(targetName,schema,table,dbIndex,tableIndex,index));
        }

        ShardingFuntion shardingFuntion = Json.decodeValue(shardingInfoText, ShardingFuntion.class);

        shardingTableConfig.setCreateTableSQL(sql);
        shardingTableConfig.setFunction(shardingFuntion);
        shardingTableConfig.setPartition(ShardingBackEndTableInfoConfig.builder().data(partitions).build());
        shardingTableConfig.setShardingIndexTables(Collections.emptyMap());

        return shardingTableConfig;
    }

    public void add(ActionEvent actionEvent) {
        try {
            shardingTableConfigVO.getIndexTables().add(this);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void delete(ActionEvent actionEvent) {
        shardingTableConfigVO.getIndexTables().remove(this);
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily( toShardingTableConfig());
    }
}

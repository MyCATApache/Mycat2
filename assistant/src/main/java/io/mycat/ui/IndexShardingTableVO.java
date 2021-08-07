package io.mycat.ui;

import io.mycat.Partition;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFunction;
import io.mycat.config.ShardingTableConfig;
import io.mycat.util.StringUtil;
import io.vertx.core.json.Json;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.Stage;
import lombok.Data;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.*;

@Data
public class IndexShardingTableVO implements VO {
    public Label logicalSchemaName;
    public Label logicalTableName;
    public TextField indexName;
    public Controller controller;

    @FXML
    public TextArea indexShardingInfo;

    @FXML
    public TableView<PartitionEntry> indexPartitionsView;

    @FXML
    public TextArea indexCreateTableSQL;
    public Button inputIndexTablePartitionButton;

    private ShardingTableConfigVO shardingTableConfigVO;
    private Stage stage;

    ShardingTableConfig shardingTableConfig = new ShardingTableConfig();

    File testFile;

    @SneakyThrows
    public void inputIndexPartitions(ActionEvent actionEvent) {
        ShardingTableConfigVO.inputPartitionsWithTestFile(getIndexPartitionsView(),testFile);
    }

    public ShardingTableConfig toShardingTableConfig() {

        shardingTableConfig.setShardingIndexTables(Collections.emptyMap());


        String sql = this.indexCreateTableSQL.getText();

        String shardingInfoText = this.indexShardingInfo.getText();

        List<List> partitions = new ArrayList<>();
        for (PartitionEntry item : indexPartitionsView.getItems()) {
            Partition partition = item.toPartition();
            String targetName = partition.getTargetName();
            String schema = partition.getSchema();
            String table = partition.getTable();
            Integer dbIndex = partition.getDbIndex();
            Integer tableIndex = partition.getTableIndex();
            Integer index = partition.getIndex();
            partitions.add(Arrays.asList(targetName, schema, table, dbIndex, tableIndex, index));
        }
        ShardingFunction shardingFuntion;
        if (!StringUtil.isEmpty(shardingInfoText)){
            shardingFuntion   = Json.decodeValue(shardingInfoText, ShardingFunction.class);
        }else {
            shardingFuntion = new ShardingFunction();
        }


        shardingTableConfig.setCreateTableSQL(sql);
        shardingTableConfig.setFunction(shardingFuntion);
        shardingTableConfig.setPartition(ShardingBackEndTableInfoConfig.builder().data(partitions).build());
        shardingTableConfig.setShardingIndexTables(Collections.emptyMap());

        return shardingTableConfig;
    }

    public void add(ActionEvent actionEvent) {
        try {
            Objects.requireNonNull(getLogicalSchemaName().getText(),"schemaName must not be null");
            Objects.requireNonNull(getLogicalTableName().getText(),"tableName must not be null");
            Map<String, ShardingTableConfig> indexTables = shardingTableConfigVO.getShardingTableConfig().getShardingIndexTables();
            indexTables.put(getIndexTableName(),validate(toShardingTableConfig()));
            shardingTableConfigVO.flash();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.stage.close();
        }
    }

    @NotNull
    public String getIndexTableName() {
        return this.getLogicalTableName().getText()+ "_" + getIndexName().getText();
    }


    public void setController(Controller controller) {
        this.controller = controller;
    }

    public void setShardingTableConfigVO(ShardingTableConfigVO shardingTableConfigVO) {
        this.shardingTableConfigVO = shardingTableConfigVO;
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(toShardingTableConfig());
    }

    @Override
    public void from(String text) {
        throw new UnsupportedOperationException();
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public void close(ActionEvent actionEvent) {
        this.stage.close();
    }
}

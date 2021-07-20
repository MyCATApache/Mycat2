package io.mycat.ui;

import io.mycat.config.LogicSchemaConfig;
import javafx.fxml.FXML;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeTableView;
import lombok.Data;

@Data
public class ShardingTableConfigVO {

    @FXML
   public TextField schemaName;

    @FXML
    public  TextField tableName;

    @FXML
    public TextArea shardingInfo;

    @FXML
    public TableView partitionsView;
}

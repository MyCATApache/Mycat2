package io.mycat.ui;

import javafx.fxml.FXML;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class SingleTableVO {
    @FXML
   public TextField schemaName;
    @FXML
    public TextField tableName;
    @FXML
    public TextField targetName;
    @FXML
    public TextField phySchemaName;
    @FXML
    public TextField phyTableName;

}

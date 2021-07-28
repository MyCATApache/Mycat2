package io.mycat.ui;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class SingleTableVO implements VO{
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
    @FXML
    public TextArea createTableSQL;

    public Controller controller;

    public void save(ActionEvent actionEvent) {
        String schemaName = this.getSchemaName().getText();
        String tableName = this.getTableName().getText();

        String targetName = this.getTargetName().getText();
        String phySchemaName = this.getPhySchemaName().getText();
        String phyTableName = this.getPhyTableName().getText();

        String sql = this.getCreateTableSQL().getText();
        controller.save(schemaName,tableName,sql,targetName,phySchemaName,phyTableName);
    }

    @Override
    public String toJsonConfig() {
        return null;
    }
}

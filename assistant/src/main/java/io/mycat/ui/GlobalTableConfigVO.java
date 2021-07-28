package io.mycat.ui;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GlobalTableConfigVO implements VO{
    @FXML
   public TextField schemaName;
    @FXML
    public TextField tableName;
    @FXML
    public ListView<String> targets;
    @FXML
    public TextArea createTableSQL;

    public Controller controller;

    public void save(ActionEvent actionEvent) {
        String schemaName = getSchemaName().getText();
        String tableName = getTableName().getText();
        String sql = getCreateTableSQL().getText();

        List<String> targets = new ArrayList<>();
        for (String item : getTargets().getItems()) {
            targets.add(item);
        }
        controller.save(schemaName,tableName,sql,targets);
    }

    @Override
    public String toJsonConfig() {
        return null;
    }
}

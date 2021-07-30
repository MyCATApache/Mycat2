package io.mycat.ui;

import io.mycat.Partition;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.GlobalTableConfig;
import io.vertx.core.json.Json;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.ListView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.TextFieldListCell;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

@Data
public class GlobalTableConfigVO implements VO {
    @FXML
    public TextField schemaName;
    @FXML
    public TextField tableName;
    @FXML
    public ListView<String> targets;
    @FXML
    public TextArea createTableSQL;

    public Controller controller;

    public GlobalTable globalTable;

    public void save(ActionEvent actionEvent) {
        String schemaName = getSchemaName().getText();
        String tableName = getTableName().getText();

        controller.save(schemaName, tableName,  getGlobalTableConfig());
    }

    @NotNull
    private GlobalTableConfig getGlobalTableConfig() {
        String sql = getCreateTableSQL().getText();

        List<GlobalBackEndTableInfoConfig> globalBackEndTableInfoConfigs = new ArrayList<>();
        for (String item : getTargets().getItems()) {
            globalBackEndTableInfoConfigs.add(GlobalBackEndTableInfoConfig.builder().targetName(item).build());
        }

        GlobalTableConfig globalTableConfig = new GlobalTableConfig();
        globalTableConfig.setCreateTableSQL(sql);
        globalTableConfig.setBroadcast(globalBackEndTableInfoConfigs);
        return globalTableConfig;
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getGlobalTableConfig());
    }

    public void setGlobalTable(GlobalTable globalTable) {
        this.globalTable = globalTable;

        getSchemaName().setText(globalTable.getSchemaName());
        getTableName().setText(globalTable.getTableName());

        ListView tableView = this.getTargets();

        tableView.setCellFactory(TextFieldListCell.forListView());
        tableView.setEditable(true);

        GlobalTableConfig globalTableConfig = globalTable.getTableConfig();

        for (GlobalBackEndTableInfoConfig globalBackEndTableInfoConfig : globalTableConfig.getBroadcast()) {
            String targetName = globalBackEndTableInfoConfig.getTargetName();
            tableView.getItems().add(targetName);
        }
    }
}

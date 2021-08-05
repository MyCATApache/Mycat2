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
import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class GlobalTableConfigVO implements VO {
    @FXML
    public TextField schemaName;
    @FXML
    public TextField tableName;
    @FXML
    public TextArea targets;
    @FXML
    public TextArea createTableSQL;

    public Controller controller;

    GlobalTableConfig globalTableConfig = new GlobalTableConfig();

    public void setGlobalTableConfig(GlobalTableConfig globalTableConfig) {
        this.globalTableConfig = globalTableConfig;

        getTargets().setText(globalTableConfig.getBroadcast().stream().map(i -> i.getTargetName()).collect(Collectors.joining(",")));
        getCreateTableSQL().setText(globalTableConfig.getCreateTableSQL());

    }

    public void save(ActionEvent actionEvent) {
        String schemaName = Objects.requireNonNull(getSchemaName().getText());
        String tableName = Objects.requireNonNull(getTableName().getText());

        controller.save(schemaName, tableName, getGlobalTableConfig());
        setGlobalTableConfig(getGlobalTableConfig());
    }

    @NotNull
    private GlobalTableConfig getGlobalTableConfig() {
        String sql = getCreateTableSQL().getText();

        List<GlobalBackEndTableInfoConfig> globalBackEndTableInfoConfigs = new ArrayList<>();
        for (String item : getTargets().getText().split(",")) {
            globalBackEndTableInfoConfigs.add(GlobalBackEndTableInfoConfig.builder().targetName(item).build());
        }
        globalTableConfig.setCreateTableSQL(sql);
        globalTableConfig.setBroadcast(globalBackEndTableInfoConfigs);
        return globalTableConfig;
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getGlobalTableConfig());
    }

    @Override
    public void from(String text) {
        setGlobalTableConfig(Json.decodeValue(text, globalTableConfig.getClass()));

    }

}

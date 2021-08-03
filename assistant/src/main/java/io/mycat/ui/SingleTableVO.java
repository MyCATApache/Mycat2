package io.mycat.ui;

import io.mycat.Partition;
import io.mycat.config.NormalBackEndTableInfoConfig;
import io.mycat.config.NormalTableConfig;
import io.vertx.core.json.Json;
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

    NormalTableConfig normalTableConfig = new NormalTableConfig();

    public NormalTableConfig getNormalTableConfig() {
        String schemaName = this.getSchemaName().getText();
        String tableName = this.getTableName().getText();
        String targetName = this.getTargetName().getText();
        String phySchemaName = this.getPhySchemaName().getText();
        String phyTableName = this.getPhyTableName().getText();
        String sql = this.getCreateTableSQL().getText();

        normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                .schemaName(phySchemaName).tableName(phyTableName).targetName(targetName).build());
        normalTableConfig.setCreateTableSQL(sql);

        return normalTableConfig;
    }

    public void setNormalTableConfig(NormalTableConfig normalTableConfig) {
        this.normalTableConfig = normalTableConfig;


        this.getTargetName().setText(normalTableConfig.getLocality().getTargetName());
        this.getPhySchemaName().setText(normalTableConfig.getLocality().getSchemaName());
        this.getPhyTableName().setText(normalTableConfig.getLocality().getTableName());
        this.getCreateTableSQL().setText(normalTableConfig.getCreateTableSQL());

    }

    public void save(ActionEvent actionEvent) {
        String schemaName = this.getSchemaName().getText();
        String tableName = this.getTableName().getText();

        String targetName = this.getTargetName().getText();
        String phySchemaName = this.getPhySchemaName().getText();
        String phyTableName = this.getPhyTableName().getText();

        String sql = this.getCreateTableSQL().getText();

        NormalTableConfig normalTableConfig = new NormalTableConfig();
        normalTableConfig.setCreateTableSQL(sql);
        normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                .targetName(targetName)
                .schemaName(phySchemaName)
                .tableName(phyTableName).build());

        controller.save(schemaName,tableName,normalTableConfig);
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getNormalTableConfig());
    }

    @Override
    public void from(String text) {
        setNormalTableConfig(Json.decodeValue(text, normalTableConfig.getClass()));
    }
}

package io.mycat.ui;

import io.mycat.config.DatasourceConfig;
import io.vertx.core.json.Json;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class DatasourceVO implements VO {
    @FXML
    public TextField name;
    @FXML
    public TextField type;
    @FXML
    public TextField user;
    @FXML
    public TextField url;
    @FXML
    public TextField dbType;
    @FXML
    public PasswordField password;

    public Controller controller;


    DatasourceConfig datasourceConfig = new DatasourceConfig();

    public void setDatasourceConfig(DatasourceConfig datasourceConfig) {
        this.datasourceConfig = datasourceConfig;

        this.getName().setText(datasourceConfig.getName());
        this.getUrl().setText(datasourceConfig.getUrl());
        this.getDbType().setText(datasourceConfig.getDbType());
        this.getType().setText(datasourceConfig.getType());
        this.getUser().setText(datasourceConfig.getUser());
        this.getPassword().setText(datasourceConfig.getPassword());
    }

    public void save(ActionEvent actionEvent) {
        controller.saveDatasource(validate(getDatasourceConfig()));
        controller.flashClusterAndDataSource();
    }

    @NotNull
    private DatasourceConfig getDatasourceConfig() {
        String name = getName().getText();
        String type = getType().getText();
        String user = getUser().getText();
        String url = getUrl().getText();
        String dbType = getDbType().getText();
        String password = getPassword().getText();

        datasourceConfig.setName(name);
        datasourceConfig.setType(type);
        datasourceConfig.setUser(user);
        datasourceConfig.setUrl(url);
        datasourceConfig.setDbType(dbType);
        datasourceConfig.setPassword(password);
        return datasourceConfig;
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getDatasourceConfig());
    }

    @Override
    public void from(String text) {
        setDatasourceConfig(Json.decodeValue(text,datasourceConfig.getClass()));
    }
}

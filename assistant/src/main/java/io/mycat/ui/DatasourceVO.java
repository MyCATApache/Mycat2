package io.mycat.ui;

import io.mycat.config.DatasourceConfig;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
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

    public Controller controller;

    public void save(ActionEvent actionEvent) {
        controller.saveDatasource(getDatasourceConfig());
    }

    @NotNull
    private DatasourceConfig getDatasourceConfig() {
        String name = getName().getText();
        String type = getType().getText();
        String user = getUser().getText();
        String url = getUrl().getText();
        String dbType = getDbType().getText();

        DatasourceConfig datasourceConfig = new DatasourceConfig();
        datasourceConfig.setName(name);
        datasourceConfig.setType(type);
        datasourceConfig.setUser(user);
        datasourceConfig.setUrl(url);
        datasourceConfig.setDbType(dbType);
        return datasourceConfig;
    }

    @Override
    public String toJsonConfig() {
        return null;
    }
}

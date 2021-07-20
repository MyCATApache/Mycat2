package io.mycat.ui;

import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class DatasourceVO {
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
}

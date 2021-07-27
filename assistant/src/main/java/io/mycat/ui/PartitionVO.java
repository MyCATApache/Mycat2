package io.mycat.ui;

import javafx.fxml.FXML;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class PartitionVO {
    @FXML
    public TextField id;
    @FXML
    public TextField target;
    @FXML
    public TextField schema;
    @FXML
    public TextField table;
}
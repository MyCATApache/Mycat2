package io.mycat.ui;

import javafx.event.ActionEvent;
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class LocalConnectionVO {
    public TextField localConnectionName;
    public TextField filePath;
    public Button connect;
    private Controller controller;

    public void connect(ActionEvent actionEvent) {
        System.out.println();
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }
}

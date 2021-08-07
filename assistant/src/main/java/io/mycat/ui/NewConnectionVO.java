package io.mycat.ui;

import javafx.event.ActionEvent;
import javafx.scene.control.Button;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class NewConnectionVO {

    public TextField newConnectionName;
    public TextArea url;
    public TextField user;
    public PasswordField password;
    public Controller controller;
    public Button connect;

    public void connect(ActionEvent actionEvent) {
        System.out.println();
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }
}

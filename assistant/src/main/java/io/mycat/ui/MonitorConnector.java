package io.mycat.ui;

import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class MonitorConnector {
    public TextField monitorIp;
    public TextField monitorPort;
    public Button instanceMonitorButton;
    public Button dbMonitorButton;
    public Button replicaMonitorButton;
    public TextField monitorName;
    public Button allButton;
}

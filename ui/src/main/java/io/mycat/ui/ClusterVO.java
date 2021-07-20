package io.mycat.ui;

import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import lombok.Data;

@Data
public class ClusterVO {
    public TextField name;
    public TextField type;
    public ListView<String> masterList;
    public ListView<String>  replicaList;
}

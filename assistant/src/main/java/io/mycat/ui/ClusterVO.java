package io.mycat.ui;

import io.mycat.config.ClusterConfig;
import javafx.event.ActionEvent;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ClusterVO implements VO{
    public TextField name;
    public TextField type;
    public ListView<String> masterList;
    public ListView<String>  replicaList;
    public Controller controller;

    @Override
    public String toJsonConfig() {
        return null;
    }
    public void save(ActionEvent actionEvent) {
        String name = this.getName().getText();
        String replicaType = (getType().getText().toUpperCase());
        List<String> masterList = new ArrayList<>(this.masterList.getItems());
        List<String> replicaList = new ArrayList<>(this.replicaList.getItems());
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setName(name);
        clusterConfig.setClusterType(replicaType);
        clusterConfig.setMasters(masterList);
        clusterConfig.setReplicas(replicaList);
        controller.saveCluster(clusterConfig);
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }
}

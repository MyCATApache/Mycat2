package io.mycat.ui;

import io.mycat.config.ClusterConfig;
import io.mycat.util.StringUtil;
import io.vertx.core.json.Json;
import javafx.event.ActionEvent;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import javax.validation.ConstraintViolation;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class ClusterVO implements VO{
    public TextField name;
    public TextField type;
    public ListView<String> masterList;
    public ListView<String>  replicaList;
    public Controller controller;

    ClusterConfig clusterConfig = new ClusterConfig();

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;

        this.getName().setText(clusterConfig.getName());
        this.getType().setText(clusterConfig.getClusterType());

        this.getMasterList().getItems().clear();
        this.getReplicaList().getItems().clear();

        this.getMasterList().getItems().addAll(Optional.ofNullable(clusterConfig.getMasters()).orElse(Collections.emptyList()));
        this.getReplicaList().getItems().addAll(Optional.ofNullable(clusterConfig.getReplicas()).orElse(Collections.emptyList()));
    }

    @Override
    public String toJsonConfig() {
        return Json.encodePrettily(getClusterConfig());
    }

    @Override
    public void from(String text) {
        setClusterConfig(Json.decodeValue(text,clusterConfig.getClass()));
    }

    public void save(ActionEvent actionEvent) {
        controller.saveCluster(validate(getClusterConfig()));
        controller.flashClusterAndDataSource();
    }



    @NotNull
    public ClusterConfig getClusterConfig() {
        String name = this.getName().getText();
        String replicaType = (getType().getText().toUpperCase());
        List<String> masterList = new ArrayList<>(this.masterList.getItems());
        List<String> replicaList = new ArrayList<>(this.replicaList.getItems());
        clusterConfig.setName(name);
        clusterConfig.setClusterType(replicaType);
        clusterConfig.setMasters(masterList);
        clusterConfig.setReplicas(replicaList);
        return clusterConfig;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }
}

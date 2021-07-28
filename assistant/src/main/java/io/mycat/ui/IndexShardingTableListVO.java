package io.mycat.ui;

import io.mycat.LogicTableType;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import lombok.Data;

@Data
public class IndexShardingTableListVO implements VO{
    public Label schemaName;
    public Label tableName;
    public ListView indexTableList;
    public Controller controller;

    public void save(ActionEvent actionEvent) {

    }

    public void inputPartitions(ActionEvent actionEvent) {

    }

    public void add(ActionEvent actionEvent) {
        try {
            FXMLLoader loader = UIMain.loader("/indexShardingTable.fxml");
            Parent parent = loader.load();
            ShardingTableConfigVO controller = loader.getController();
            String schemaName = getSchemaName().getText();
            String tableName = getTableName().getText();

            controller.getSchemaName().setText(schemaName);
            controller.getTableName().setText(tableName);

            Stage stage = new Stage();
            Scene scene = new Scene(parent, 400, 200);
            stage.setScene(scene);
            stage.show();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void delete(ActionEvent actionEvent) {

    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    @Override
    public String toJsonConfig() {
        return null;
    }
}

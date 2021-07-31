package io.mycat.ui;

import com.google.common.collect.ImmutableMap;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class MainPaneVO {
    public AnchorPane mainPane;
    public MenuBar menu;
    public TabPane tabPane;
    public HBox runMenu;
    public TextArea inputSql;
    public TextArea explain;
    public TextArea output;
    public Label statusMessage;
    public Map<String, Controller> tabObjectMap = new HashMap<>();

    public void flashRoot() {

        Menu fileMenu = new Menu("文件");
        MenuItem newConnection = new MenuItem("新连接");
        newConnection.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    final Stage dialog = new Stage();


                    FXMLLoader loader = UIMain.loader("/newConnection.fxml");
                    Parent parent = loader.load();

                    NewConnectionVO newConnectionVO = loader.getController();

                    Scene dialogScene = new Scene(parent, 600, 500);
                    dialog.setScene(dialogScene);
                    dialog.setTitle("新连接");
                    newConnectionVO.getConnect().setOnAction(new EventHandler<ActionEvent>() {
                        @Override
                        public void handle(ActionEvent event) {

                            try {
                                String name = CheckUtil.isEmpty(newConnectionVO.getName().getText(), "name 不能为空");
                                String ip = CheckUtil.isEmpty(newConnectionVO.getIp().getText(), "ip 不能为空");
                                String port = CheckUtil.isEmpty(newConnectionVO.getPort().getText(), "port 不能为空");
                                String user = CheckUtil.isEmpty(newConnectionVO.getUser().getText(), "user 不能为空");
                                String password = CheckUtil.isEmpty(newConnectionVO.getPassword().getText(), "password 不能为空");
                                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                                builder.put("ip", ip);
                                builder.put("port", port);
                                builder.put("user", user);
                                builder.put("password", password);
                                ImmutableMap<String, Object> map = builder.build();

                                FXMLLoader loader = UIMain.loader("/mainpane.fxml");
                                Parent parent = loader.load();

                                Controller controller = loader.getController();
                                controller.setInfoProvider(UIMain.getInfoProviderFactory().create(InfoProviderType.TCP, map));
                                controller.flashRoot();
                                controller.getMain().prefWidthProperty().bind(tabPane.widthProperty());//菜单自适应
                                controller.getMain().prefHeightProperty().bind(tabPane.heightProperty());//菜单自适应

                                Tab tab = new Tab(name, parent);
                                tabPane.getTabs().add(tab);
                                dialog.close();
                            } catch (Exception e) {
                                popAlter(e);
                                e.printStackTrace();
                            }

                        }
                    });
                    dialog.showAndWait();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        });
        MenuItem newTestConnection = new MenuItem("本地连接");
        newTestConnection.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    final Stage dialog = new Stage();


                    FXMLLoader loader = UIMain.loader("/localConnection.fxml");
                    Parent parent = loader.load();

                    LocalConnectionVO newConnectionVO = loader.getController();

                    Scene dialogScene = new Scene(parent, 600, 500);
                    dialog.setScene(dialogScene);
                    dialog.setTitle("本地连接");
                    newConnectionVO.getConnect().setOnAction(new EventHandler<ActionEvent>() {
                        @Override
                        public void handle(ActionEvent event) {
                            try {
                                String name = CheckUtil.isEmpty(newConnectionVO.getName().getText(), "name 不能为空");
                                String filePath = CheckUtil.isEmpty(newConnectionVO.getFilePath().getText(), "filePath不能为空");
                                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                                builder.put("filePath", filePath);
                                ImmutableMap<String, Object> map = builder.build();

                                FXMLLoader loader = UIMain.loader("/mainpane.fxml");
                                Parent parent = loader.load();

                                Controller controller = loader.getController();
                                controller.setInfoProvider(UIMain.getInfoProviderFactory().create(InfoProviderType.LOCAL, map));
                                controller.flashRoot();
                                controller.getMain().prefWidthProperty().bind(tabPane.widthProperty());//菜单自适应
                                controller.getMain().prefHeightProperty().bind(tabPane.heightProperty());//菜单自适应
                                tabObjectMap.put(name, controller);
                                Tab tab = new Tab(name, parent);
                                tabPane.getTabs().add(tab);
                                dialog.close();
                            } catch (Exception e) {
                                popAlter(e);
                                e.printStackTrace();
                            }

                        }
                    });
                    dialog.showAndWait();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        });
        fileMenu.getItems().addAll(newConnection, newTestConnection);

        Menu helpMenu = new Menu("帮助");
        MenuItem aboutMenu = new MenuItem("关于");
        aboutMenu.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                final Stage dialog = new Stage();
                dialog.initModality(Modality.APPLICATION_MODAL);
                VBox dialogVbox = new VBox(20);
                dialogVbox.getChildren().add(new TextField("github:https://github.com/MyCATApache/Mycat2 "));
                dialogVbox.getChildren().add(new Label("author:chenjunwen"));
                Scene dialogScene = new Scene(dialogVbox, 300, 200);
                dialog.setScene(dialogScene);
                dialog.setTitle("关于");
                dialog.showAndWait();
            }
        });
        helpMenu.getItems().addAll(aboutMenu);

        menu.getMenus().addAll(fileMenu, helpMenu);

        Button runBotton = new Button("执行");

        AtomicReference<Tab> selectTab = new AtomicReference<>();
        tabPane.getSelectionModel().selectedItemProperty().addListener((obs, ov, nv) -> {
            selectTab.set(nv);
        });

        runBotton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                Tab s = selectTab.get();
                if (s != null) {
                    String text = s.getText();
                    Controller controller = tabObjectMap.get(text);
                    InfoProvider infoProvider = controller.getInfoProvider();
                    String sql = inputSql.getText();
                    List<Map<String, Object>> map = infoProvider.query(sql);
                    output.setText(map.toString());
                }
            }
        });
        runMenu.getChildren().addAll(runBotton);


    }

    private void popAlter(Exception e) {
        Stage stage = new Stage();
        stage.initModality(Modality.APPLICATION_MODAL);
        stage.setTitle("警告");
        stage.setScene(new Scene(new Label(e.getLocalizedMessage()), 200, 100));
        stage.showAndWait();
    }
}

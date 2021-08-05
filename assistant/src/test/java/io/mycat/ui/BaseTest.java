package io.mycat.ui;

import io.mycat.config.DatasourceConfig;
import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.GlobalTableConfig;
import io.mycat.hint.DropDataSourceHint;
import io.vertx.core.json.Json;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseEvent;
import javafx.stage.Stage;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.testfx.api.FxRobot;
import org.testfx.api.FxRobotInterface;
import org.testfx.framework.junit.ApplicationTest;
import org.testfx.robot.Motion;
import org.testfx.service.query.NodeQuery;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import static io.mycat.ui.SceneUtil.lookupNode;
import static io.mycat.ui.SceneUtil.lookupTextNode;

public class BaseTest extends ApplicationTest {

    private UIMain uiMain;
    private Stage stage;

    /**
     * Will be called with {@code @Before} semantics, i. e. before each test method.
     */
    @Override
    @SneakyThrows
    public void start(Stage stage) {
        this.stage = stage;
        uiMain = new UIMain();
        uiMain.start(stage);
    }

    @org.junit.Test
    public void testCreateSingleTable() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupNode("#objectTree").ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                lookupNode("#schemas").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                String mycat = ObjectItem.ofSchema("mycat").getId();
                lookupNode(mycat).ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                lookupNode(ObjectItem.ofSingleTables("mycat").getId()).ifPresent(c -> {
                    rightClickOn(c);
                    robot.moveTo("#addSingleTable", Motion.DEFAULT);
                    robot.clickOn("#addSingleTable");
                    robot.interrupt();
                });
                lookupTextNode("#schemaName").ifPresent(c -> {
                    c.setText("mycat");
                });
                lookupTextNode("#tableName").ifPresent(c -> {
                    c.setText("address");
                });
                lookupTextNode("#createTableSQL").ifPresent(c -> {
                    c.setText("\n" +
                            "CREATE TABLE `address` (\n" +
                            "  `id` int(11) NOT NULL,\n" +
                            "  `addressname` varchar(20) DEFAULT NULL,\n" +
                            "  PRIMARY KEY (`id`)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
                });
                lookupTextNode("#targetName").ifPresent(c -> {
                    c.setText("prototype");
                });
                lookupTextNode("#phySchemaName").ifPresent(c -> {
                    c.setText("mycat");
                });
                lookupTextNode("#phyTableName").ifPresent(c -> {
                    c.setText("address");
                });
                lookupNode("#saveSingleTable").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });

                System.out.println();
            }
        });

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofSingleTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        Optional<Node> node = lookupNode(ObjectItem.ofSingleTable("mycat", "address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });
        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));
        Assert.assertEquals("prototype", lookupTextNode("#targetName").get().getText());
        Assert.assertEquals("mycat", lookupTextNode("#phySchemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#phyTableName").get().getText());

        lookupTextNode("#phyTableName").ifPresent(c -> {
            c.setText("ccc");
        });
        lookupNode("#saveSingleTable").ifPresent(c -> {
            clickOn(c);
            robot.interrupt();
        });

        node = lookupNode(ObjectItem.ofSingleTable("mycat", "address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });
        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));
        Assert.assertEquals("prototype", lookupTextNode("#targetName").get().getText());
        Assert.assertEquals("mycat", lookupTextNode("#phySchemaName").get().getText());
        Assert.assertEquals("ccc", lookupTextNode("#phyTableName").get().getText());

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofSingleTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofSingleTable("mycat", "address").getId()).ifPresent(c -> {
            rightClickOn(c);
            robot.moveTo("#deleteSingleTable", Motion.DEFAULT);
            robot.clickOn("#deleteSingleTable");
            robot.interrupt();
        });
        lookupNode("enter").ifPresent(c -> {
            clickOn(c);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofSingleTable("mycat", "address").getId()).ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                System.out.println();
                Assert.assertNull(schemaObjectCell.getTreeItem());
            }
        });

        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testCreateGlobalTable() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupNode("#objectTree").ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                lookupNode("#schemas").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                String mycat = ObjectItem.ofSchema("mycat").getId();
                lookupNode(mycat).ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                lookupNode(ObjectItem.ofGlobalTables("mycat").getId()).ifPresent(c -> {
                    rightClickOn(c);
                    robot.moveTo("#addGlobalTable", Motion.DEFAULT);
                    robot.clickOn("#addGlobalTable");
                    robot.interrupt();
                });
                String config
                        = Json.encodePrettily(GlobalTableConfig.builder()
                        .createTableSQL("\n" +
                                "CREATE TABLE `address` (\n" +
                                "  `id` int(11) NOT NULL,\n" +
                                "  `addressname` varchar(20) DEFAULT NULL,\n" +
                                "  PRIMARY KEY (`id`)\n" +
                                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                        .broadcast(Arrays.asList(GlobalBackEndTableInfoConfig.builder()
                                .targetName("prototype")
                                .build())).build());
                lookupTextNode("#tableName").ifPresent(c -> {
                    c.setText("address");
                    robot.interrupt();
                });
                lookupTextNode("#objectText").ifPresent(c -> {
                    c.setText(config);
                    robot.interrupt();
                });
                Controller.INSTANCE.textToNax.handle(null);
                robot.interrupt();
                lookupNode("#saveGlobalTable").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });

                System.out.println();
            }
        });

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofGlobalTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        Optional<Node> node = lookupNode(ObjectItem.ofGlobalTable("mycat", "address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });
        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));

        lookupTextNode("#targets").ifPresent(c -> {
            c.setText("prototype,prototype");
        });
        robot.interrupt();
        lookupNode("#saveGlobalTable").ifPresent(c -> {
            clickOn(c);
            robot.interrupt();
        });
        node = lookupNode(ObjectItem.ofSchema("mycat").getId());
        node.ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        node = lookupNode(ObjectItem.ofGlobalTables("mycat").getId());
        node.ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        node = lookupNode(ObjectItem.ofGlobalTable("mycat", "address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });
        robot.interrupt();
        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));
        lookupNode("targets").ifPresent(c -> {
            String text = ((TextArea) c).getText();
            Assert.assertEquals("prototype,prototype", text);
            System.out.println();
        });

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofGlobalTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofGlobalTable("mycat", "address").getId()).ifPresent(c -> {
            rightClickOn(c);
            robot.moveTo("#deleteGlobalTable", Motion.DEFAULT);
            robot.clickOn("#deleteGlobalTable");
            robot.interrupt();
        });
        lookupNode("enter").ifPresent(c -> {
            clickOn(c);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofGlobalTable("mycat", "address").getId()).ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                System.out.println();
                Assert.assertNull(schemaObjectCell.getTreeItem());
            }
        });

        System.out.println(sceneSet);
    }


    @org.junit.Test
    public void testCreateDatasource() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupNode("#objectTree").ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                lookupNode("#datasources").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                rightClickOn(lookupNode("#datasources").get());
                robot.moveTo("#addDatasource", Motion.DEFAULT);
                robot.clickOn("#addDatasource");
                robot.interrupt();

                String url = "jdbc:mysql://localhost:3306/db1?useServerPrepStmts=false&useCursorFetch=true&serverTimezone=Asia/Shanghai&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";

                DatasourceConfig datasourceConfig = new DatasourceConfig();
                datasourceConfig.setDbType("mysql");
                datasourceConfig.setName("ds3");
                datasourceConfig.setUser("root");
                datasourceConfig.setPassword("123456");
                datasourceConfig.setUrl(url);
                String config = Json.encode(datasourceConfig);
                lookupTextNode("#objectText").ifPresent(c -> {
                    c.setText(config);
                    robot.interrupt();
                });
                Controller.INSTANCE.textToNax.handle(null);
                robot.interrupt();
                lookupNode("#saveDatasource").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });

                lookupNode("#datasources").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                Optional<Node> ds3Node = lookupNode(ObjectItem.ofDatasource("ds3").getId());
                Assert.assertTrue(ds3Node.isPresent());
                ds3Node.ifPresent(n->{
                    rightClickOn(ds3Node.get());
                });
                robot.interrupt();
            }
        });
        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testTcpConnectionLogin() {
        String name = "t";
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        clickOn("#file");
        clickOn("#newTCPConnection");
        lookupTextNode("#name").ifPresent(c -> c.setText(name));
        lookupTextNode("#newTCPConnection").ifPresent(text -> {
            text.setText("n");
        });
        lookupTextNode("#url").ifPresent(text -> {
            text.setText("jdbc:mysql://localhost:8066/mysql?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
        });
        lookupTextNode("#user").ifPresent(text -> {
            text.setText("root");
        });
        lookupTextNode("#password").ifPresent(text -> {
            text.setText("123456");
        });
        lookupNode("#connect").ifPresent(node -> clickOn(node));
        lookupNode("#tabPane").ifPresent(node -> {
            TabPane tabPane = (TabPane) node;
            boolean present = tabPane.getTabs().stream().filter(i -> i.getText().equalsIgnoreCase(name)).findFirst().isPresent();
            Assert.assertTrue(present);
            System.out.println();
        });
    }
}
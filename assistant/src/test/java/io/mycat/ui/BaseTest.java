package io.mycat.ui;

import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
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
        System.out.println(sceneSet);
    }

    private void sleep() {
        try {
            Thread.sleep(111);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
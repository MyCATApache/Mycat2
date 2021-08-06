package io.mycat.ui;

import au.com.bytecode.opencsv.CSVWriter;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.GlobalBackEndTableInfoConfig;
import io.mycat.config.GlobalTableConfig;
import io.mycat.config.ShardingFunction;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateSchemaHint;
import io.mycat.router.mycat1xfunction.PartitionByRangeMod;
import io.vertx.core.json.Json;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.stage.Stage;
import lombok.SneakyThrows;
import org.apache.groovy.util.Maps;
import org.junit.Assert;
import org.testfx.api.FxRobot;
import org.testfx.framework.junit.ApplicationTest;
import org.testfx.robot.Motion;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
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
                Node node1 = lookupNode("#datasources").get();
                lookupNode("#datasources").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                Optional<Node> ds3Node = lookupNode(ObjectItem.ofDatasource("ds3").getId());
                Assert.assertTrue(ds3Node.isPresent());
                ds3Node.ifPresent(n -> {
                    rightClickOn(ds3Node.get());
                    robot.moveTo("#deleteDatasource", Motion.DEFAULT);
                    robot.clickOn("#deleteDatasource");
                });
                robot.interrupt();
                lookupNode("enter").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });
                Optional<Node> deletedDs3Node = lookupNode(ObjectItem.ofDatasource("ds3").getId());
                deletedDs3Node.ifPresent(new Consumer<Node>() {
                    @Override
                    public void accept(Node node) {
                        SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                        System.out.println();
                        Assert.assertNull(schemaObjectCell.getTreeItem());
                    }
                });
            }
        });
        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testCreateCluster() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupNode("#objectTree").ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                lookupNode("#clusters").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                rightClickOn(lookupNode("#clusters").get());
                robot.moveTo("#addCluster", Motion.DEFAULT);
                robot.clickOn("#addCluster");
                robot.interrupt();

                String config = Json.encode(CreateClusterHint.createConfig("c3", Arrays.asList("prototypeDs"), Arrays.asList()));
                lookupTextNode("#objectText").ifPresent(c -> {
                    c.setText(config);
                    robot.interrupt();
                });
                Controller.INSTANCE.textToNax.handle(null);
                robot.interrupt();
                lookupNode("#saveCluster").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });
                Node node1 = lookupNode("#clusters").get();
                lookupNode("#clusters").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                Optional<Node> c3Node = lookupNode(ObjectItem.ofCluster("c3").getId());
                Assert.assertTrue(c3Node.isPresent());
                c3Node.ifPresent(n -> {
                    rightClickOn(c3Node.get());
                    robot.moveTo("#deleteCluster", Motion.DEFAULT);
                    robot.clickOn("#deleteCluster");
                });
                robot.interrupt();
                lookupNode("enter").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });
                Optional<Node> deletedDs3Node = lookupNode(ObjectItem.ofDatasource("c3").getId());
                deletedDs3Node.ifPresent(new Consumer<Node>() {
                    @Override
                    public void accept(Node node) {
                        SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                        System.out.println();
                        Assert.assertNull(schemaObjectCell.getTreeItem());
                    }
                });
            }
        });
        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testExecuteSQL() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupTextNode("#inputSql").ifPresent(c -> c.setText("select 1"));

        clickOn(lookupNode("#runButton").get());
        robot.interrupt();
        Optional<TextInputControl> textInputControl = lookupTextNode("#output");
        Assert.assertEquals(" 1  |\n" +
                "-----\n" +
                " 1  |\n", textInputControl.get().getText());
        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testFlashButton() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();

        clickOn(lookupNode("#flashRootButton").get());
        robot.interrupt();
        System.out.println(sceneSet);
    }

    @org.junit.Test
    public void testCreateSchema() {
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
                rightClickOn(lookupNode("#schemas").get());
                robot.moveTo("#addSchema", Motion.DEFAULT);
                robot.clickOn("#addSchema");
                robot.interrupt();

                String config = Json.encode(CreateSchemaHint.createConfig("s3", null));
                lookupTextNode("#objectText").ifPresent(c -> {
                    c.setText(config);
                    robot.interrupt();
                });
                Controller.INSTANCE.textToNax.handle(null);
                robot.interrupt();
                lookupNode("#saveSchema").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });
                Node node1 = lookupNode("#schemas").get();
                lookupNode("#schemas").ifPresent(c -> {
                    SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
                    schemaObjectCell.getTreeItem().setExpanded(true);
                    robot.interrupt();
                });
                Optional<Node> s3Node = lookupNode(ObjectItem.ofSchema("s3").getId());
                Assert.assertTrue(s3Node.isPresent());
                s3Node.ifPresent(n -> {
                    rightClickOn(s3Node.get());
                    robot.moveTo("#deleteSchema", Motion.DEFAULT);
                    robot.clickOn("#deleteSchema");
                });
                robot.interrupt();
                lookupNode("enter").ifPresent(c -> {
                    clickOn(c);
                    robot.interrupt();
                });
                Optional<Node> deletedS3Node = lookupNode(ObjectItem.ofSchema("s3").getId());
                deletedS3Node.ifPresent(new Consumer<Node>() {
                    @Override
                    public void accept(Node node) {
                        SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                        System.out.println();
                        Assert.assertNull(schemaObjectCell.getTreeItem());
                    }
                });
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


    @org.junit.Test
    @SneakyThrows
    public void testCreateShardingTable() {
        FxRobot robot = new FxRobot();
        Set<Scene> sceneSet = SceneUtil.sceneSet;
        testTcpConnectionLogin();
        lookupNode("#objectTree").ifPresent(new Consumer<Node>() {
            @Override
            @SneakyThrows
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
                lookupNode(ObjectItem.ofShardingTables("mycat").getId()).ifPresent(c -> {
                    rightClickOn(c);
                    robot.moveTo("#addShardingTable", Motion.DEFAULT);
                    robot.clickOn("#addShardingTable");
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
                robot.interrupt();
                String functionConfig = Json.encodePrettily(ShardingFunction.builder()
                        .clazz(PartitionByRangeMod.class.getCanonicalName())
                        .properties(Maps.of(
                                "defaultNode", -1,
                                "columnName", "id"))
                        .ranges(Maps.of(
                                "0-1", 0,
                                "1-300", 1)).build());
                lookupTextNode("#shardingInfo").ifPresent(c -> {
                    c.setText(functionConfig);
                });


                robot.interrupt();
                Path tempFile = Files.createTempFile("inputPartition", "csv");
                System.out.println(tempFile);
                StringWriter stringWriter = new StringWriter();
                CSVWriter csvWriter = new CSVWriter(stringWriter);
                csvWriter.writeNext("prototype", "s0", "t0", "0", "0", "0");
                csvWriter.writeNext("prototype", "s0", "t1", "0", "1", "1");
                csvWriter.flush();
                csvWriter.close();

                Files.write(tempFile, stringWriter.getBuffer().toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                ShardingTableConfigVO currentVO =(ShardingTableConfigVO) Controller.INSTANCE.getCurrentVO();
                currentVO.testFile = tempFile.toFile();
                clickOn("#inputPartitionButton");
                robot.interrupt();
                clickOn("#saveShardingTable");
                robot.interrupt();
                System.out.println();
            }
        });

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofShardingTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        Optional<Node> node = lookupNode(ObjectItem.ofShardingTable("mycat", "address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });
        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));
        Assert.assertTrue(!lookupTextNode("#shardingInfo").get().getText().isEmpty());
        Assert.assertTrue(!lookupNode("#partitionsView").map(i-> (TableView)i).get().getItems().isEmpty());


        //test index table
        clickOn("#addIndexTable");
        lookupTextNode("#indexName").ifPresent(c -> {
            c.setText("index_t");
            robot.interrupt();
        });
        lookupTextNode("#indexCreateTableSQL").ifPresent(c -> {
            c.setText("\n" +
                    "CREATE TABLE `address_index_t` (\n" +
                    "  `id` int(11) NOT NULL,\n" +
                    "  PRIMARY KEY (`id`)\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
            robot.interrupt();
        });
        robot.interrupt();
        robot.interrupt();
        String functionConfig = Json.encodePrettily(ShardingFunction.builder()
                .clazz(PartitionByRangeMod.class.getCanonicalName())
                .properties(Maps.of(
                        "defaultNode", -1,
                        "columnName", "id"))
                .ranges(Maps.of(
                        "0-1", 0,
                        "1-300", 1)).build());
        lookupTextNode("#indexShardingInfo").ifPresent(c -> {
            c.setText(functionConfig);
        });

        Path tempFile = Files.createTempFile("inputPartition", "csv");
        System.out.println(tempFile);
        StringWriter stringWriter = new StringWriter();
        CSVWriter csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeNext("prototype", "s0", "t0", "0", "0", "0");
        csvWriter.writeNext("prototype", "s0", "t1", "0", "1", "1");
        csvWriter.flush();
        csvWriter.close();

        Files.write(tempFile, stringWriter.getBuffer().toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        ShardingTableConfigVO currentVO =(ShardingTableConfigVO) Controller.INSTANCE.getCurrentVO();
        currentVO.getCurrentIndexShardingTableController().testFile = tempFile.toFile();

        clickOn("#inputIndexTablePartitionButton");

        robot.interrupt();
        clickOn("#saveIndexTable");
        robot.interrupt();
        System.out.println();
        clickOn("#saveShardingTable");


        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofShardingTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });

        node = lookupNode(ObjectItem.ofShardingTable("mycat","address").getId());
        Assert.assertTrue(node.isPresent());
        node.ifPresent(c -> {
            clickOn(c);
        });


        Assert.assertEquals("mycat", lookupTextNode("#schemaName").get().getText());
        Assert.assertEquals("address", lookupTextNode("#tableName").get().getText());
        Assert.assertTrue(lookupTextNode("#createTableSQL").get().getText().contains("addressname"));


        ShardingTableConfigVO shardingTableConfigVO = (ShardingTableConfigVO)Controller.INSTANCE.getCurrentVO();
        Object o = shardingTableConfigVO.getIndexTableList().getItems().get(0);
        shardingTableConfigVO.getIndexTableList().getSelectionModel().select(o);
        clickOn("#deleteIndexTable");
        robot.interrupt();
        Assert.assertTrue(shardingTableConfigVO.getIndexTableList().getItems().isEmpty());
        clickOn("#saveShardingTable");

        lookupNode(ObjectItem.ofSchema("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofShardingTables("mycat").getId()).ifPresent(c -> {
            SchemaObjectCell schemaObjectCell = (SchemaObjectCell) c;
            schemaObjectCell.getTreeItem().setExpanded(true);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofShardingTable("mycat", "address").getId()).ifPresent(c -> {
            rightClickOn(c);
            robot.moveTo("#deleteShardingTable", Motion.DEFAULT);
            robot.clickOn("#deleteShardingTable");
            robot.interrupt();
        });
        lookupNode("enter").ifPresent(c -> {
            clickOn(c);
            robot.interrupt();
        });
        lookupNode(ObjectItem.ofShardingTable("mycat", "address").getId()).ifPresent(new Consumer<Node>() {
            @Override
            public void accept(Node node) {
                SchemaObjectCell schemaObjectCell = (SchemaObjectCell) node;
                System.out.println();
                Assert.assertNull(schemaObjectCell.getTreeItem());
            }
        });

        System.out.println(sceneSet);
    }
}
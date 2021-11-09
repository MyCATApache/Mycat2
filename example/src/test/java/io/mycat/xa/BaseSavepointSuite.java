package io.mycat.xa;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SavepointSqlConnection;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.function.BiFunction;

public  abstract class BaseSavepointSuite extends XaTestSuite{
    public BaseSavepointSuite(MySQLManager mySQLManager, BiFunction<MySQLManager, XaLog, XaSqlConnection> factory) throws Exception {
        super(mySQLManager, factory);
    }
    @Test
    public void baseSavepointCommit(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {
                Future<Void> sss = savepointSqlConnection.createSavepoint("sss");
                sss.toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();
                ;
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void baseSavepointRollback(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {
                Future<Void> sss = savepointSqlConnection.createSavepoint("sss");
                sss.toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollback();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void baseSavepointRelease(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {
                Future<Void> sss = savepointSqlConnection.createSavepoint("sss");
                sss.toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.releaseSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void baseSavepointRollbackSavepoint(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {
                Future<Void> sss = savepointSqlConnection.createSavepoint("sss");
                sss.toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollbackSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointCommitInSingleConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();


                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);


                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);
                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointRollbackInSingleConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();


                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);


                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollback()
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);
                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointRollbackSavepointInSingleConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();


                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);


                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollbackSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertTrue(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);


                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointReleaseSavepointInSingleConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();


                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);


                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.releaseSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertTrue(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);


                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointReleaseSavepointInTwoConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        mySQLManager.getConnection("ds2").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();

        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                RowSet objects2 = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects2.size() > 0);

                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.releaseSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                objects2 = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects2.size() > 0);

                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertTrue(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                ds2 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }


    @Test
    @SneakyThrows
    public void baseSavepointRollbackSavepointInTwoConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        mySQLManager.getConnection("ds2").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();

        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                RowSet objects2 = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects2.size() > 0);

                ds1.update("delete FROM `db1`.`travelrecord`")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.update("delete FROM `db1`.`travelrecord`")
                        .toCompletionStage().toCompletableFuture().get();

                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollbackSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertTrue(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                ds2 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    @SneakyThrows
    public void baseSavepointCommitSavepointInTwoConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        mySQLManager.getConnection("ds2").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();

        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                RowSet objects2 = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects2.size() > 0);

                ds1.update("delete FROM `db1`.`travelrecord`")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.update("delete FROM `db1`.`travelrecord`")
                        .toCompletionStage().toCompletableFuture().get();

                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.commit()
                        .toCompletionStage().toCompletableFuture().get();

                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);

                ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }


    @Test
    @SneakyThrows
    public void baseSavepointRollbackInTwoConnection(VertxTestContext testContext) {
        mySQLManager.getConnection("ds1").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();
        mySQLManager.getConnection("ds2").flatMap(connection -> {
            return connection.update("delete FROM `db1`.`travelrecord`").map(u -> connection);
        }).flatMap(c -> c.close()).toCompletionStage().toCompletableFuture().get();

        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager, xaLog);
        Assert.assertTrue(baseXaSqlConnection instanceof SavepointSqlConnection);

        SavepointSqlConnection savepointSqlConnection = (SavepointSqlConnection) baseXaSqlConnection;

        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            @SneakyThrows
            public void handle(AsyncResult<Void> event) {

                NewMycatConnection ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();

                ds1.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                NewMycatConnection ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();

                ds2.insert("insert into `db1`.`travelrecord` (`id`) values ('2')")
                        .toCompletionStage().toCompletableFuture().get();

                savepointSqlConnection.createSavepoint("sss")
                        .toCompletionStage().toCompletableFuture().get();

                RowSet objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() > 0);

                RowSet objects2 = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects2.size() > 0);


                ds2.insert("insert into `db1`.`travelrecord` (`id`) values ('3')")
                        .toCompletionStage().toCompletableFuture().get();


                Assert.assertEquals("[sss]", savepointSqlConnection.getExistedSavepoints().toString());
                savepointSqlConnection.rollback()
                        .toCompletionStage().toCompletableFuture().get();

                Assert.assertEquals("[]", savepointSqlConnection.getExistedSavepoints().toString());
                Assert.assertFalse(savepointSqlConnection.isInTransaction());

                ds1 = savepointSqlConnection.getConnection("ds1")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds1.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);

                ds2 = savepointSqlConnection.getConnection("ds2")
                        .toCompletionStage().toCompletableFuture().get();
                objects = ds2.query("select * from `db1`.`travelrecord` where id = 2")
                        .toCompletionStage().toCompletableFuture().get();
                Assert.assertTrue(objects.size() == 0);

                savepointSqlConnection.close();
                testContext.completeNow();
            }
        });
    }
}

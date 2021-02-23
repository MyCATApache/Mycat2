package io.mycat.commands;

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.MetaClusterCurrent;
import io.mycat.ScheduleUtil;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.vertxmycat.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.impl.JDBCRow;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.vertx.core.Future.succeededFuture;

public class MycatMySQLManager implements MySQLManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatMySQLManager.class);
    private static final ExecutorService IO_EXECUTOR = Executors.newCachedThreadPool();

    public MycatMySQLManager() {

    }

    //
    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        DefaultConnection connection = jdbcConnectionManager.getConnection(targetName);
        return succeededFuture(new AbstractMySqlConnection() {
            @Override
            public MySQLConnection exceptionHandler(Handler<Throwable> handler) {
                throw new UnsupportedOperationException();
            }

            @Override
            public MySQLConnection closeHandler(Handler<Void> handler) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> ping() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> specifySchema(String schemaName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> resetConnection() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<PreparedStatement> prepare(String sql) {
                return Future.succeededFuture(
                        new MycatVertxPreparedStatement(sql, this));
            }

            @Override
            public Query<RowSet<Row>> query(String sql) {
                return new Query<RowSet<Row>>() {
                    @Override
                    public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
                        Future<RowSet<Row>> future = execute();
                        if (handler != null) {
                            future.onComplete(handler);
                        }
                    }

                    @Override
                    @SneakyThrows
                    public Future<RowSet<Row>> execute() {
                        return Future.future(event -> {
                            IO_EXECUTOR.submit(() -> {
                                try {
                                    event.complete(innerExecute());
                                } catch (Throwable throwable) {
                                    event.tryFail(throwable);
                                }
                            });
                        });
                    }

                    @NotNull
                    private RowSet<Row> innerExecute() throws SQLException {
                        Connection rawConnection = connection.getRawConnection();
                        Statement statement = rawConnection.createStatement();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("MycatMySQLManager targetName:{} sql:{} rawConnection:{}", targetName, sql,rawConnection);
                        }
                        if (!statement.execute(sql, Statement.RETURN_GENERATED_KEYS)) {
                            VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
                            vertxRowSet.setAffectRow(statement.getUpdateCount());
                            ResultSet generatedKeys = statement.getGeneratedKeys();
                            if (generatedKeys != null) {
                                if (generatedKeys.next()) {
                                    Number object = (Number) generatedKeys.getObject(1);
                                    if (object != null) {
                                        vertxRowSet.setLastInsertId(object.longValue());
                                    }
                                }
                            }
                            return (vertxRowSet);
                        }
                        ResultSet resultSet = statement.getResultSet();
                        JdbcRowMetaData metaData = new JdbcRowMetaData(
                                resultSet.getMetaData());
                        int columnCount = metaData.getColumnCount();
                        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
                        for (int i = 0; i < columnCount; i++) {
                            int index = i;
                            columnDescriptors.add(new ColumnDescriptor() {
                                @Override
                                public String name() {
                                    return metaData.getColumnName(index);
                                }

                                @Override
                                public boolean isArray() {
                                    return false;
                                }

                                @Override
                                public JDBCType jdbcType() {
                                    return JDBCType.valueOf(metaData.getColumnType(index));
                                }
                            });
                        }
                        VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
                        RowDesc rowDesc = new RowDesc(metaData.getColumnList(), columnDescriptors);
                        while (resultSet.next()) {
                            JDBCRow jdbcRow = new JDBCRow(rowDesc);
                            for (int i = 0; i < columnCount; i++) {
                                jdbcRow.addValue(resultSet.getObject(i+1));
                            }
                            vertxRowSet.list.add(jdbcRow);
                        }
                        return (vertxRowSet);
                    }

                    @Override
                    public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
                        return new Query<SqlResult<R>>() {
                            @Override
                            public void execute(Handler<AsyncResult<SqlResult<R>>> handler) {
                                Future<SqlResult<R>> future = execute();
                                if (handler != null) {
                                    future.onComplete(handler);
                                }
                            }

                            @Override
                            @SneakyThrows
                            public Future<SqlResult<R>> execute() {
                                Connection rawConnection = connection.getRawConnection();
                                return Future.future(new Handler<Promise<SqlResult<R>>>() {
                                    @Override
                                    public void handle(Promise<SqlResult<R>> promise) {
                                        IO_EXECUTOR.submit(() -> extracted(promise));
                                    }

                                    @SneakyThrows
                                    private void extracted(Promise<SqlResult<R>> promise) {
                                        try (Statement statement = rawConnection.createStatement()) {

                                            LOGGER.debug("MycatMySQLManager targetName:{} sql:{}", targetName, sql);

                                            statement.execute(sql);
                                            ResultSet resultSet = statement.getResultSet();
                                            JdbcRowMetaData metaData = new JdbcRowMetaData(
                                                    resultSet.getMetaData());
                                            int columnCount = metaData.getColumnCount();
                                            List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
                                            for (int i = 0; i < columnCount; i++) {
                                                int index = i;
                                                columnDescriptors.add(new ColumnDescriptor() {
                                                    @Override
                                                    public String name() {
                                                        return metaData.getColumnName(index);
                                                    }

                                                    @Override
                                                    public boolean isArray() {
                                                        return false;
                                                    }

                                                    @Override
                                                    public JDBCType jdbcType() {
                                                        return JDBCType.valueOf(metaData.getColumnType(index));
                                                    }
                                                });
                                            }

                                            RowDesc rowDesc = new RowDesc(metaData.getColumnList(), columnDescriptors);
                                            ColumnDefPacket[] columnDefPackets = new ColumnDefPacket[columnCount];
                                            for (int i = 0; i < columnCount; i++) {
                                                columnDefPackets[i] = new ColumnDefPacketImpl(metaData, i);
                                            }


                                            if (collector instanceof StreamMysqlCollector) {
                                                MySQLRowDesc mySQLRowDesc = new MySQLRowDesc(
                                                        Arrays.asList(columnDefPackets).stream().map(packet -> {
                                                            String catalog = new String(packet.getColumnCatalog());
                                                            String schema = new String(packet.getColumnSchema());
                                                            String table = new String(packet.getColumnTable());
                                                            String orgTable = new String(packet.getColumnOrgTable());
                                                            String name = new String(packet.getColumnName());
                                                            String orgName = new String(packet.getColumnOrgName());
                                                            int characterSet = packet.getColumnCharsetSet();
                                                            long columnLength = packet.getColumnLength();
                                                            DataType type = DataType.valueOf(packet.getColumnType());
                                                            int flags = packet.getColumnFlags();
                                                            byte decimals = packet.getColumnDecimals();
                                                            ColumnDefinition columnDefinition = new ColumnDefinition(
                                                                    catalog,
                                                                    schema,
                                                                    table,
                                                                    orgTable,
                                                                    name,
                                                                    orgName,
                                                                    characterSet,
                                                                    columnLength,
                                                                    type,
                                                                    flags,
                                                                    decimals
                                                            );
                                                            return columnDefinition;
                                                        }).toArray(n -> new ColumnDefinition[n]), DataFormat.TEXT);
                                                ((StreamMysqlCollector) collector)
                                                        .onColumnDefinitions(mySQLRowDesc);
                                            }
                                            {
                                                Object supplier = collector.supplier().get();
                                                BiConsumer<Object, Row> accumulator = (BiConsumer) collector.accumulator();
                                                Function<Object, Object> finisher = (Function) collector.finisher();
                                                int count = 0;
                                                while (resultSet.next()) {
                                                    JDBCRow jdbcRow = new JDBCRow(rowDesc);
                                                    for (int i = 0; i < columnCount; i++) {
                                                        jdbcRow.addValue(resultSet.getObject(i + 1));
                                                    }
                                                    count++;
                                                    accumulator.accept(supplier, jdbcRow);
                                                }
                                                finisher.apply(supplier);
                                                promise.complete(new MySqlResult<>(
                                                        count, 0, 0, (R) supplier, columnDescriptors));
                                            }
                                        } catch (Throwable throwable) {
                                            promise.tryFail(throwable);
                                        }
                                    }
                                });


                            }

                            @Override
                            public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }

                    @Override
                    public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
                return new RowSetMySqlPreparedQuery(sql, this);
            }

            @Override
            public Future<Void> close() {
                connection.close();
                return Future.succeededFuture();
            }
        });
    }

    @Override
    public Future<Map<String, SqlConnection>> getConnectionMap() {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        return getMapFuture(new HashSet<>(connectionManager.getDatasourceInfo().keySet()));
    }

    //
//        public Future<SqlConnection> getConnection(String targetName) {
//
//        PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
//        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
//        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
//        sqlDatasourcePool.createSession().onComplete(event -> {
//            MycatWorkerProcessor workerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
//            NameableExecutor mycatWorker = workerProcessor.getMycatWorker();
//            if (event.failed()){
//                mycatWorker.execute(()->promise.tryFail(event.cause()));
//            }else {
//                mycatWorker.execute(()-> promise.tryComplete(new AbstractMySqlConnectionImpl(event.result())));
//            }
//        });
//        return promise.future();
//    }
    @Override
    public Future<Void> close() {
        return succeededFuture();
    }

    @Override
    public void setTimer(long delay, Runnable handler) {
        ScheduleUtil.getTimer().schedule(() -> {
            handler.run();
            return null;
        }, delay, TimeUnit.MILLISECONDS);
    }
}

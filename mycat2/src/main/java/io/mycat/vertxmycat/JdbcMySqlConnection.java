package io.mycat.vertxmycat;

import io.mycat.MetaClusterCurrent;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.impl.JDBCRow;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.codec.VertxRowSetImpl;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class JdbcMySqlConnection extends AbstractMySqlConnection {
    private static final ReadWriteThreadPool IO_EXECUTOR = new ReadWriteThreadPool(
            JdbcMySqlConnection.class.getSimpleName(),
            1000,
            300,
            10 * 1000
    );
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMySqlConnection.class);
    private final DefaultConnection connection;
    private final String targetName;

    public JdbcMySqlConnection(String targetName) {
        this.targetName = targetName;
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        this.connection = jdbcConnectionManager.getConnection(targetName);
    }

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
        return new JdbcQuery(sql);
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
        return new RowSetMySqlPreparedQuery(sql, this);
    }

    @Override
    public Future<Void> close() {
        IO_EXECUTOR.execute(true,()->{
            connection.close();
        });
        return Future.succeededFuture();
    }

    class JdbcQuery implements Query<RowSet<Row>> {
        private final String sql;
        private final boolean isRead;

        public JdbcQuery(String sql) {
            this.sql = sql;
            this.isRead = isRead(sql);
        }

        @Override
        public String toString() {
            return sql;
        }

        private boolean isRead(String sql) {
            return sql.contains("select") || sql.contains("SELECT");
        }

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
                IO_EXECUTOR.execute(isRead, () -> {
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
                LOGGER.debug("MycatMySQLManager targetName:{} sql:{} rawConnection:{}", targetName, sql, rawConnection);
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
                    jdbcRow.addValue(resultSet.getObject(i + 1));
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
                            IO_EXECUTOR.execute(isRead, () -> extracted(promise));
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
                                                DataType type = DataType.valueOf(packet.getColumnType()==15?253:packet.getColumnType());
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
    }
}

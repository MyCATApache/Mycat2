/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertxmycat;

import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.impl.JDBCRow;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.codec.VertxRowSetImpl;
import io.vertx.mysqlclient.impl.datatype.DataFormat;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.adaptType;
import static io.mycat.vertxmycat.AbstractMySqlConnectionImpl.toObjects;

public class RowSetJdbcPreparedJdbcQuery implements AbstractMySqlPreparedQuery<RowSet<Row>> {
    private String targetName;
    private final String sql;
    private final Connection connection;
    private ReadWriteThreadPool threadPool;
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySqlConnectionImpl.class);

    public RowSetJdbcPreparedJdbcQuery(String targetName, String sql, Connection connection, ReadWriteThreadPool threadPool) {
        this.targetName = targetName;
        this.sql = sql;
        this.connection = connection;
        this.threadPool = threadPool;
    }

    private RowSet<Row> innerExecute(Tuple tuple) throws SQLException {
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug(sql);
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            setParams(tuple, preparedStatement);
            if (!preparedStatement.execute()) {
                VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
                vertxRowSet.setAffectRow(preparedStatement.getUpdateCount());
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
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
            ResultSet resultSet = preparedStatement.getResultSet();
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
    }

    public static void setParams(Tuple tuple, PreparedStatement preparedStatement) throws SQLException {
        if (tuple.size() > 0) {
            List<Object> list = new ArrayList<>();
            for (Object v : toObjects(tuple)) {
                Object o = adaptType(v);
                list.add(o);
            }
            MycatPreparedStatementUtil.setParams(preparedStatement, list);
        }
    }

    @Override
    public Future<RowSet<Row>> execute(Tuple tuple) {
        return Future.future(promise -> threadPool.execute(true, () -> {
            try {
                promise.complete(innerExecute(tuple));
            } catch (SQLException throwables) {
                promise.tryFail(throwables);
            }
        }));
    }

    @Override
    public Future<RowSet<Row>> executeBatch(List<Tuple> batch) {
        return Future.future(promise -> threadPool.execute(true, () -> {
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                for (Tuple tuple : batch) {
                    setParams(tuple, preparedStatement);
                    preparedStatement.addBatch();
                }
                VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
                vertxRowSet.setAffectRow(Arrays.stream(preparedStatement.executeBatch()).sum());
                long lastInsertId = 0;
                try {
                    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                    while (generatedKeys.next()) {
                        BigDecimal aLong = generatedKeys.getBigDecimal(1);
                        lastInsertId = aLong.longValue();
                    }
                } catch (Throwable throwable) {
                    LOGGER.error("", throwable);
                }
                vertxRowSet.setLastInsertId(lastInsertId);
                promise.tryComplete(vertxRowSet);
            } catch (Throwable throwable) {
                promise.tryFail(throwable);
            }
        }));

    }

    @Override
    public Future<RowSet<Row>> execute() {
        return execute(Tuple.tuple());
    }

    @Override
    public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
        return new SqlResultCollectingPrepareJdbcQuery<R>(sql, connection, collector,threadPool);
    }

    @Override
    public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> function) {
        throw new UnsupportedOperationException();
    }


    public static <R> void extracted(Promise<SqlResult<R>> promise, Statement statement, ResultSet resultSet, Collector<Row, ?, R> collector) throws SQLException {
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
                        DataType type = DataType.valueOf(packet.getColumnType() == 15 ? 253 : packet.getColumnType());
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
            resultSet.close();
            statement.close();
            promise.complete(new MySqlResult<>(
                    count, 0, 0, (R) supplier, columnDescriptors));
        }
    }
}

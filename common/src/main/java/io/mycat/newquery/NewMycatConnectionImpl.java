package io.mycat.newquery;

import com.alibaba.druid.pool.DruidConnectionHolder;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.util.JdbcUtils;
import com.mysql.cj.result.Field;
import io.mycat.MySQLPacketUtil;
import io.mycat.beans.mycat.*;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.mycat.beans.mycat.MycatDataType.BINARY;

@Getter
public class NewMycatConnectionImpl implements NewMycatConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewMycatConnectionImpl.class);

    boolean needLastInsertId;
    private String targetName;
    Connection connection;
    ResultSet resultSet;
    Future<Void> future = Future.succeededFuture();
    boolean isMySQLDriver = false;
    private String dbType;
    public NewMycatConnectionImpl(boolean needLastInsertId, Connection connection) {
        this.needLastInsertId = needLastInsertId;
        this.connection = connection;
    }

    public NewMycatConnectionImpl(String targetName, Connection connection, String dbType) {
        this.targetName = targetName;
        this.connection = connection;
        this.needLastInsertId = true;
        this.dbType = dbType;
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        return Future.future(new Handler<Promise<RowSet>>() {
            @Override
            public void handle(Promise<RowSet> rowSetPromise) {
                prepareQuery(sql, params, new MysqlCollector() {
                    MycatRowMetaData mycatRowMetaData;
                    ArrayList<Object[]> objects = new ArrayList<>();

                    @Override
                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                        this.mycatRowMetaData = mycatRowMetaData;
                    }

                    @Override
                    public void onRow(Object[] row) {
                        objects.add(row);
                    }

                    @Override
                    public void onComplete() {
                        RowSet rowSet = new RowSet(mycatRowMetaData, objects);
                        rowSetPromise.tryComplete(rowSet);
                    }

                    @Override
                    public void onError(Throwable e) {
                        rowSetPromise.fail(e);
                    }
                });
            }
        });
    }

    @Override
    public synchronized void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        if (this.future.isComplete()) {
            this.future = Future.succeededFuture();
        }
        this.future = this.future.transform(voidAsyncResult -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("targetName:{}\n sql:{}\n params:{}", targetName, sql, params);
            }
            try {
                if (params.isEmpty()) {
                    try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                        setStreamFlag(statement);
                        resultSet = statement.executeQuery(sql);
                        onRev();
                        MycatRowMetaData mycatRowMetaData = getJdbcRowMetaData(resultSet.getMetaData());
                        int columnCount = mycatRowMetaData.getColumnCount();
                        collector.onColumnDef(mycatRowMetaData);
                        int columnLimit = columnCount + 1;
                        while (!isResultSetClosed() && resultSet.next()) {
                            Object[] objects = new Object[columnCount];
                            for (int i = 1, j = 0; i < columnLimit; i++, j++) {
                                objects[j] = resultSet.getObject(i);
                            }
                            collector.onRow(objects);
                        }
                    }
                } else {
                    try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
                        setStreamFlag(statement);
                        int limit = params.size() + 1;
                        for (int i = 1; i < limit; i++) {
                            statement.setObject(i, params.get(i - 1));
                        }
                        onSend();
                        resultSet = statement.executeQuery();
                        onRev();
                        MycatRowMetaData mycatRowMetaData = getJdbcRowMetaData(resultSet.getMetaData());
                        int columnCount = mycatRowMetaData.getColumnCount();
                        collector.onColumnDef(mycatRowMetaData);
                        int columnLimit = columnCount + 1;
                        while (!isResultSetClosed() && resultSet.next()) {
                            Object[] objects = new Object[columnCount];
                            for (int i = 1, j = 0; i < columnLimit; i++, j++) {
                                objects[j] = resultSet.getObject(i);
                            }
                            collector.onRow(objects);
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("", e);
                collector.onError(e);
                return Future.failedFuture(e);
            } finally {
                resultSet = null;
            }
            collector.onComplete();
            return Future.succeededFuture();
        });

    }

    @SneakyThrows
    private void setStreamFlag(Statement statement) {
        if (statement.toString().contains("mysql") || statement.getClass().getName().contains("mysql")) {
            statement.setFetchSize(Integer.MIN_VALUE);
        }
//        protected boolean createStreamingResultSet() {
//            return ((this.query.getResultType() == Resultset.Type.FORWARD_ONLY) && (this.resultSetConcurrency == java.sql.ResultSet.CONCUR_READ_ONLY)
//                    && (this.query.getResultFetchSize() == Integer.MIN_VALUE));
//        }
    }

    private boolean isResultSetClosed() {
        try {
            return resultSet == null || resultSet.isClosed();
        } catch (Exception ignored) {
            LOGGER.error("", ignored);
            return true;
        }
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, MycatRelDataType mycatRelDataType, BufferAllocator allocator) {
        return Observable.create(emitter -> {
            synchronized (NewMycatConnectionImpl.this) {
                if (this.future.isComplete()) {
                    this.future = Future.succeededFuture();
                }
                NewMycatConnectionImpl.this.future = NewMycatConnectionImpl.this.future.transform(voidAsyncResult -> {
                    try {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("targetName:{}\n sql:{}\n params:{}", targetName, sql, params);
                        }
                        try (PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            setStreamFlag(statement);
                            int limit = params.size() + 1;
                            for (int i = 1; i < limit; i++) {
                                statement.setObject(i, params.get(i - 1));
                            }
                            onSend();
                            resultSet = statement.executeQuery();
                            onRev();
                            MycatField[] mycatFields;
                            if (mycatRelDataType == null){
                                mycatFields = MycatDataType.from(resultSet.getMetaData());
                            }else {
                                mycatFields = mycatRelDataType.getFieldList().toArray(new MycatField[0]);
                            }
                            VectorSchemaRoot vectorSchemaRoot = null;
                            FieldVector[] fieldVectors = null;
                            int rowId = 0;
                            while (resultSet.next()) {
                                if (vectorSchemaRoot == null) {
                                    fieldVectors = getFieldVectors(mycatFields, allocator);
                                    vectorSchemaRoot = new VectorSchemaRoot(Arrays.asList(fieldVectors));
                                    vectorSchemaRoot.allocateNew();
                                }
                                for (int j = 0; j < mycatFields.length; j++) {
                                    mycatFields[j].getMycatDataType()
                                            .convertToVector(resultSet, j, fieldVectors[j], rowId);
                                }

                                rowId++;
                                vectorSchemaRoot.setRowCount(rowId);
                                if (rowId > 1024) {
                                    emitter.onNext(vectorSchemaRoot);
                                    rowId = 0;
                                    vectorSchemaRoot = null;
                                }
                            }
                            if (vectorSchemaRoot != null) {
                                emitter.onNext(vectorSchemaRoot);
                                vectorSchemaRoot = null;
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("", e);
                        emitter.onError(e);
                        return Future.failedFuture(e);
                    } finally {
                        resultSet = null;
                    }
                    emitter.onComplete();
                    return Future.succeededFuture();
                });
            }
        });
    }


    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return prepareQuery(sql,params,null,allocator);
    }

    public static String paramize(String sql, List<Object> params) {
        if (params.isEmpty()) return sql;
        if (sql.startsWith("be")) {
            return sql;
        }
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            int index;

            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    if (index < params.size()) {
                        Object value = params.get(index++);
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, io.mycat.PreparedStatement.fromJavaObject(value));
                    }
                }
                super.endVisit(x);
            }
        });
        sql = sqlStatement.toString();
        return sql;
    }

    @Override
    public Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus) {
        return Observable.create(emitter -> {
            synchronized (NewMycatConnectionImpl.this) {
                this.future = this.future.transform(new Function<AsyncResult<Void>, Future<Void>>() {
                    private Buffer packet(byte[] generateMySQLPacket) {
                        return Buffer.buffer(generateMySQLPacket);
                    }

                    @Override
                    public Future<Void> apply(AsyncResult<Void> voidAsyncResult) {
                        try {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("targetName:{}\n sql:{}\n params:{}", targetName, sql, params);
                            }
                            String paramize = paramize(sql, params);
                            boolean clientDeprecateEof = NewMycatConnectionConfig.CLIENT_DEPRECATE_EOF;
                            try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                                NewMycatConnectionImpl.this.setStreamFlag(statement);
                                resultSet = statement.executeQuery(paramize);
                                NewMycatConnectionImpl.this.onRev();
                                int columnCount = resultSet.getMetaData().getColumnCount();
                                int packetId = 1;
                                MycatRowMetaData metaData = getJdbcRowMetaData(resultSet.getMetaData());
                                MycatRelDataType mycatRelDataType = metaData.getMycatRelDataType();
                                List<MycatField> fieldList = mycatRelDataType.getFieldList();
                                emitter.onNext(packet(
                                        MySQLPacketUtil.generateMySQLPacket(packetId++,
                                                MySQLPacketUtil.generateResultSetCount(metaData.getColumnCount()))
                                ));
                                Iterable<byte[]> bytes = MySQLPacketUtil.generateAllColumnDefPayload(metaData);
                                for (byte[] aByte : bytes) {
                                    emitter.onNext(packet(
                                            MySQLPacketUtil.generateMySQLPacket(packetId++,
                                                    aByte)
                                    ));
                                }
                                if (!clientDeprecateEof) {
                                    emitter.onNext(packet(
                                            MySQLPacketUtil.generateMySQLPacket(packetId++,
                                                    MySQLPacketUtil.generateEof(0, 0))
                                    ));
                                }
                                while (!isResultSetClosed() && resultSet.next()) {
                                    byte[][] row = new byte[columnCount][];
                                    for (int index = 0; index < columnCount; index += 1) {
                                        if (isMySQLDriver) {
                                            int jdbcColumnIndex = index + 1;
//                                            MycatDataType mycatDataType = mycatField.getMycatDataType();
//                                            if (mycatDataType == DATE) {
//                                                ResultSetImpl resultSet = NewMycatConnectionImpl.this.resultSet.unwrap(ResultSetImpl.class);
//                                                Date localDate = resultSet.getDate(jdbcColumnIndex);
//                                                row[index] = localDate == null ? null :localDate.toString().getBytes();
//                                            } else if (mycatDataType == DATETIME) {
//                                                ResultSetImpl resultSet = NewMycatConnectionImpl.this.resultSet.unwrap(ResultSetImpl.class);
//                                                Timestamp localDateTime = ((ResultSetImpl) resultSet).getTimestamp(jdbcColumnIndex);
//                                                row[index] = localDateTime == null ? null :localDateTime.toString().getBytes();
//                                            } else {
//                                                row[index] = (resultSet.getBytes(jdbcColumnIndex));
//                                            }
                                            row[index] = (resultSet.getBytes(jdbcColumnIndex));
                                        } else {
                                            MycatField mycatField = fieldList.get(index);
                                            if (mycatField.getMycatDataType() == BINARY) {
                                                row[index] = (resultSet.getBytes(index + 1));
                                            } else {
                                                String string = resultSet.getString(index + 1);
                                                row[index] = string == null ? null : string.getBytes();
                                            }
                                        }
                                    }
                                    emitter.onNext(Buffer.buffer(MySQLPacketUtil.generateMySQLPacket(packetId++, MySQLPacketUtil.generateTextRow(row))));
                                }
                                if (clientDeprecateEof) {
                                    emitter.onNext(packet(
                                            MySQLPacketUtil.generateMySQLPacket(packetId++,
                                                    MySQLPacketUtil.generateOk(0xfe, 0, 0, 0, 0, false, false, false, ""))
                                    ));
                                } else {
                                    emitter.onNext(packet(
                                            MySQLPacketUtil.generateMySQLPacket(packetId++,
                                                    MySQLPacketUtil.generateEof(0, 0))
                                    ));
                                }
                                emitter.onComplete();
                            } catch (Exception e) {
                                emitter.tryOnError(e);
                            }
                        } catch (Throwable e) {
                            return Future.failedFuture(e);
                        }
                        return Future.succeededFuture();
                    }
                });
            }
        });


    }

    public static byte[] getBytes(LocalDateTime value) {
        int year = value.getYear();
        int monthValue = value.getMonthValue();
        int dayOfMonth = value.getDayOfMonth();
        int hour = value.getHour();
        int minute = value.getMinute();
        int second = value.getSecond();
        int nano = value.getNano() / 1000;
        if (nano == 0) {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    year,
                    monthValue,
                    dayOfMonth,
                    hour,
                    minute,
                    second
            ).getBytes();
        }
        return (String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",//"%04d-%02d-%02d %02d:%02d:%02d.%09d"
                year,
                monthValue,
                dayOfMonth,
                hour,
                minute,
                second,
                nano
        )).getBytes();
    }

    @NotNull
    private FieldVector[] getFieldVectors(MycatField[] mycatFields, BufferAllocator allocator) {
        FieldVector[] fieldVectors = new FieldVector[mycatFields.length];
        int i = 0;
        for (MycatField mycatField : mycatFields) {
            String name = mycatField.getName();
            boolean nullable = mycatField.isNullable();
            ArrowType arrowType = mycatField.getMycatDataType().getArrowType();
            FieldType fieldType =
                    new FieldType(nullable, arrowType, null);
            org.apache.arrow.vector.types.pojo.Field field =
                    new org.apache.arrow.vector.types.pojo.Field(name, fieldType, Collections.emptyList());
            FieldVector fieldVector = mycatField.getMycatDataType().createFieldVector(field, allocator);
            fieldVectors[i] = fieldVector;
            i++;
        }
        return fieldVectors;
    }

    @Override
    public synchronized Future<List<Object>> call(String sql) {
        if (this.future.isComplete()) {
            this.future = Future.succeededFuture();
        }
        Future<List<Object>> transform = future.transform(voidAsyncResult -> {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("targetName:{}\n sql:{}\n", targetName, sql);
                }
                ArrayList<Object> resultSetList = new ArrayList<>();
                CallableStatement callableStatement = connection.prepareCall(sql);
                boolean moreResults = true;
                int updateCount = 0;
                callableStatement.execute();
                while (moreResults && updateCount != -1) {
                    updateCount = callableStatement.getUpdateCount();
                    if (updateCount == -1) {
                        ResultSet resultSet = callableStatement.getResultSet();
                        if (resultSet == null) {
                            break;
                        }
                        MycatRowMetaData metaData = getJdbcRowMetaData(resultSet.getMetaData());
                        List<Object[]> objects = new ArrayList<>();
                        while (resultSet.next()) {
                            int columnCount = metaData.getColumnCount();
                            Object[] row = new Object[columnCount];
                            for (int i = 0; i < columnCount; i++) {
                                row[i] = resultSet.getObject(i + 1);
                            }
                            objects.add(row);
                        }
                        RowSet rowSet = new RowSet(metaData, objects);
                        resultSetList.add(rowSet);
                    } else {
                        resultSetList.add(new long[]{
                                updateCount, getLastInsertId(callableStatement)
                        });
                    }
                    moreResults = callableStatement.getMoreResults();

                }
                return Future.succeededFuture(resultSetList);
            } catch (Exception exception) {
                LOGGER.error("", exception);
                return Future.failedFuture(exception);
            }
        });
        this.future = transform.mapEmpty();
        return transform;
    }

    @Override
    public synchronized Future<SqlResult> insert(String sql, List<Object> params) {

        if (this.future.isComplete()) {
            this.future = Future.succeededFuture();
        }
        Future<SqlResult> transform = future.transform(voidAsyncResult -> {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("targetName:{}\n sql:{}\n params:{}", targetName, sql, params);
                }
                long affectRows;
                long lastInsertId = 0;
                if (params.isEmpty()) {
                    try (Statement statement = connection.createStatement();) {
                        onSend();
                        affectRows = statement.executeUpdate(sql, needLastInsertId ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
                        onRev();
                        lastInsertId = getLastInsertId(statement);
                    }
                } else {
                    if(isClickHouse()){
                        String paramize = paramize(sql, params);
                        try (Statement statement = connection.createStatement();) {
                            onSend();
                            LOGGER.debug("sql:{}", paramize);
                            affectRows = statement.executeUpdate(paramize);
                            onRev();
                            lastInsertId= 0;
                        }
                    } else {
                        try (PreparedStatement preparedStatement = connection.prepareStatement(sql, needLastInsertId ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS)) {
                            int limit = params.size() + 1;
                            for (int i = 1; i < limit; i++) {
                                preparedStatement.setObject(i, params.get(i - 1));
                            }
                            onSend();
                            affectRows = preparedStatement.executeUpdate();
                            onRev();
                            lastInsertId = getLastInsertId(preparedStatement);
                        }
                    }
                }
                SqlResult sqlResult = new SqlResult();
                sqlResult.setAffectRows(affectRows);
                sqlResult.setLastInsertId(lastInsertId);
                return Future.succeededFuture(sqlResult);
            } catch (Exception e) {
                LOGGER.error("", e);
                return Future.failedFuture(e);
            }
        });
        this.future = transform.mapEmpty();
        return transform;
    }


    private boolean isClickHouse() {
        return dbType.toString().contains("clickhouse");
    }


    @Override
    public Future<SqlResult> insert(String sql) {
        return insert(sql, Collections.emptyList());
    }

    @Override
    public Future<SqlResult> update(String sql) {
        return update(sql, Collections.emptyList());
    }

    @Override
    public synchronized Future<SqlResult> update(String sql, List<Object> params) {
        if (this.future.isComplete()) {
            this.future = Future.succeededFuture();
        }
        Future<SqlResult> transform = future.transform(voidAsyncResult -> {
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("targetName:{}\n sql:{}\n params:{}", targetName, sql, params);
                }
                long affectRows;
                long lastInsertId = 0;
                if (params.isEmpty()) {
                    try (Statement statement = connection.createStatement();) {
                        onSend();
                        affectRows = statement.executeUpdate(sql);
                        onRev();
                        lastInsertId = 0;
                    }
                } else {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        int limit = params.size() + 1;
                        for (int i = 1; i < limit; i++) {
                            preparedStatement.setObject(i, params.get(i - 1));
                        }
                        onSend();
                        affectRows = preparedStatement.executeUpdate();
                        onRev();
                        lastInsertId = 0;
                    }
                }
                SqlResult sqlResult = new SqlResult();
                sqlResult.setAffectRows(affectRows);
                sqlResult.setLastInsertId(lastInsertId);
                return Future.succeededFuture(sqlResult);
            } catch (Exception e) {
                LOGGER.error("", e);
                return Future.failedFuture(e);
            }
        });
        this.future = transform.mapEmpty();
        return transform;
    }

    @Override
    public Future<Void> close() {
        return future.transform(result -> {
            if (resultSet != null) {
                JdbcUtils.close(resultSet);
            }
            JdbcUtils.close(connection);
            return Future.succeededFuture();
        });
    }

    @Override
    public boolean isClosed() {
        try {
            return this.connection.isClosed();
        }catch (Throwable throwable){
            LOGGER.error("",throwable);
            return true;
        }
    }

    @Override
    public void abandonConnection() {
        JdbcUtils.close(this.connection);
    }

    @Override
    public Future<Void> abandonQuery() {
        if (resultSet != null) {
            JdbcUtils.close(resultSet);
            resultSet = null;
        }
        return Future.succeededFuture();
    }

    private long getLastInsertId(Statement statement) {
        long lastInsertId = 0;
        if (needLastInsertId) {
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    lastInsertId = ((Number) generatedKeys.getObject(1)).longValue();
                }
            } catch (Exception e) {
                LOGGER.error("", e);
                lastInsertId = 0;
                //  needLastInsertId = false;
            }
        }
        return lastInsertId;
    }

    @NotNull
    private MycatRowMetaData getJdbcRowMetaData(ResultSetMetaData jdbcMetaData) throws SQLException {
        MycatRowMetaData mycatRowMetaData;
        if (isMySQLDriver || "com.mysql.cj.jdbc.result.ResultSetMetaData".equals(jdbcMetaData.getClass().getCanonicalName())) {
            isMySQLDriver = true;
            com.mysql.cj.jdbc.result.ResultSetMetaData mysqlJdbcMetaData = (com.mysql.cj.jdbc.result.ResultSetMetaData) jdbcMetaData;
            int columnCount = mysqlJdbcMetaData.getColumnCount();
            Field[] fields = mysqlJdbcMetaData.getFields();
            List<ColumnDefPacket> columnDefPackets = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
                Field field = fields[i];
                columnDefPacket.setColumnSchema(ColumnDefPacketImpl.getBytes(field.getDatabaseName()));
                columnDefPacket.setColumnTable(ColumnDefPacketImpl.getBytes(field.getTableName()));
                columnDefPacket.setColumnOrgTable(ColumnDefPacketImpl.getBytes(field.getOriginalTableName()));
                columnDefPacket.setColumnName(ColumnDefPacketImpl.getBytes(field.getName()));
                columnDefPacket.setColumnOrgName(ColumnDefPacketImpl.getBytes(field.getOriginalName()));
                columnDefPacket.setColumnCharsetSet((field.getCollationIndex()));
                columnDefPacket.setColumnLength((int) field.getLength());
                columnDefPacket.setColumnType((int) field.getMysqlTypeId());
                columnDefPacket.setColumnFlags((int) field.getFlags());
                columnDefPacket.setColumnDecimals((byte) field.getDecimals());
                columnDefPackets.add(columnDefPacket);
            }
            mycatRowMetaData = new MycatMySQLRowMetaData(columnDefPackets);
        } else {
            mycatRowMetaData = new CopyMycatRowMetaData(new JdbcRowMetaData(jdbcMetaData));
        }
        return mycatRowMetaData;
    }

}

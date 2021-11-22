package io.mycat.newquery;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.mysqlclient.impl.MySQLConnectionImpl;
import io.vertx.mysqlclient.impl.MySQLPoolImpl;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.data.Numeric;
import lombok.SneakyThrows;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class NewVertxConnectionImpl implements NewMycatConnection {
    MySQLConnectionImpl mySQLConnection;
    CursorHandler cursorHandler = null;

    public NewVertxConnectionImpl(MySQLConnectionImpl mySQLConnection) {
        this.mySQLConnection = mySQLConnection;
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        Future<RowSet> future = Future.future(new Handler<Promise<RowSet>>() {
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

        return future;
    }

    static class VectorCursorHandler implements Handler<PreparedStatement> {
        List<ColumnDefinition> columnDescriptors = null;
        Cursor cursor = null;
        private ObservableEmitter<VectorSchemaRoot> emitter;
        private List<Object> params;
        private Schema schema;
        private RootAllocator rootAllocator;

        public VectorCursorHandler(ObservableEmitter<VectorSchemaRoot> emitter, List<Object> params) {
            this.emitter = emitter;
            this.params = params;
        }

        public Future<Void> close() {
            if (cursor != null) {
                return cursor.close();
            }
            return Future.succeededFuture();
        }

        @Override
        public void handle(PreparedStatement preparedStatement) {

            // Create a cursor
            cursor = preparedStatement.cursor(Tuple.of(params));

            // Read 50 rows
            cursor.read(8192, ar2 -> {
                if (ar2.succeeded()) {
                    io.vertx.sqlclient.RowSet<Row> rows = ar2.result();
                    if (columnDescriptors == null) {
                        columnDescriptors = (List) rows.columnDescriptors();
                        this.schema = resultSetColumnToVectorRowSchema(columnDescriptors);
                        this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
                    }
                    int columnSize = columnDescriptors.size();
                    int rowCount = rows.rowCount();
                    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
                    List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
                    int rowId = 0;
                    for (Row row : rows) {
                        for (int i = 0; i < columnSize; i++) {
                            ColumnDefinition columnDefinition = columnDescriptors.get(i);
                            FieldVector valueVectors = fieldVectors.get(i);
                            Object nullValue = row.getValue(i);
                            if (nullValue == null){
                                if (valueVectors instanceof BaseFixedWidthVector){
                                    ((BaseFixedWidthVector) valueVectors).setNull(rowId);
                                }else {
                                    BaseVariableWidthVector variableWidthVector = (BaseVariableWidthVector) valueVectors;
                                    variableWidthVector.setNull(rowId);
                                }
                                continue;
                            }
                            switch (columnDefinition.type()) {
                                case INT1:
                                case INT2:
                                case INT3:
                                case INT4:
                                case INT8:{
                                    BaseIntVector baseIntVector = (BaseIntVector) valueVectors;
                                    baseIntVector.setWithPossibleTruncate(rowId,((Number)nullValue).longValue());
                                    break;
                                }
                                case DOUBLE:
                                case FLOAT: {
                                    FloatingPointVector floatingPointVector = (FloatingPointVector) valueVectors;
                                    floatingPointVector.setWithPossibleTruncate(rowId,((Number)nullValue).doubleValue());
                                    break;
                                }
                                case NUMERIC: {
                                    Numeric value = (Numeric) row.getValue(i);
                                    DecimalVector decimalVector = (DecimalVector) valueVectors;
                                    decimalVector.set(rowId,value.bigDecimalValue());
                                    break;
                                }
                                case VARSTRING:
                                case STRING: {
                                    VarCharVector varCharVector = (VarCharVector) valueVectors;
                                    Object value = row.getValue(i);
                                    if (value instanceof Buffer){
                                        varCharVector.setSafe(rowId,((Buffer) value).getBytes());
                                    }else if (value instanceof String){
                                        varCharVector.setSafe(rowId,((String) value).getBytes());
                                    }else {
                                        throw new UnsupportedOperationException();
                                    }
                                    break;
                                }
                                case TINYBLOB:
                                case BLOB:
                                case MEDIUMBLOB:
                                case LONGBLOB: {
                                    VarBinaryVector varBinaryVector = (VarBinaryVector) valueVectors;
                                    Buffer buffer = row.getBuffer(i);
                                    varBinaryVector.set(rowId,buffer.getBytes());
                                    break;
                                }
                                case DATE:
                                case TIME:
                                case DATETIME:
                                case YEAR:
                                case TIMESTAMP:
                                case BIT:
                                case UNBIND:
                                case JSON:
                                case GEOMETRY:
                                case NULL:
                                    break;
                            }
                        }
                        rowId++;
                    }
                    // Check for more ?
                    if (cursor.hasMore()) {
                        // Repeat the process...
                    } else {
                        this.emitter.onComplete();
                        // No more rows - close the cursor
                        cursor.close();
                    }
                } else {
                    this.emitter.onError(ar2.cause());
                }
            });
        }
    }

    public static Schema resultSetColumnToVectorRowSchema(List<ColumnDefinition> columnDefinitions) {
        int columnCount = columnDefinitions.size();
        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        JdbcToArrowConfigBuilder jdbcToArrowConfigBuilder = new JdbcToArrowConfigBuilder();
        JdbcToArrowConfig jdbcToArrowConfig = jdbcToArrowConfigBuilder.build();
        for (int i = 0; i < columnCount; i++) {
            ColumnDefinition columnDefinition = columnDefinitions.get(i);
            String columnName = columnDefinition.name();
            int columnType = columnDefinition.jdbcType().getVendorTypeNumber();
            boolean signed = (columnDefinition.flags() & MySQLFieldsType.UNSIGNED_FLAG) == 0;
            boolean nullable = (columnDefinition.flags() & MySQLFieldsType.UNSIGNED_FLAG) == 0;


            ArrowType arrowType = (ArrowType) jdbcToArrowConfig.getJdbcToArrowTypeConverter().apply(new JdbcFieldInfo(columnType, 0, 0));
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            builder.add(new org.apache.arrow.vector.types.pojo.Field(columnName, fieldType, Collections.emptyList()));
        }
        return new org.apache.arrow.vector.types.pojo.Schema(builder.build());
    }


    static class CursorHandler implements Handler<PreparedStatement> {
        List<ColumnDefinition> columnDescriptors = null;
        Cursor cursor = null;
        MysqlCollector collector;

        public CursorHandler(MysqlCollector collector) {
            this.collector = collector;
        }

        public Future<Void> close() {
            if (cursor != null) {
                return cursor.close();
            }
            return Future.succeededFuture();
        }

        @Override
        public void handle(PreparedStatement preparedStatement) {

            // Create a cursor
            cursor = preparedStatement.cursor(Tuple.of(18));

            // Read 50 rows
            cursor.read(8192, ar2 -> {
                if (ar2.succeeded()) {
                    io.vertx.sqlclient.RowSet<Row> rows = ar2.result();
                    if (columnDescriptors == null) {
                        columnDescriptors = (List) rows.columnDescriptors();
                        collector.onColumnDef(toColumnMetaData(columnDescriptors));
                    }
                    int size = columnDescriptors.size();
                    for (Row row : rows) {
                        Object[] objects = new Object[size];
                        for (int i = 0; i < size; i++) {
                            ColumnDefinition columnDefinition = columnDescriptors.get(i);
                            switch (columnDefinition.type()) {
                                case INT1:
                                case INT2:
                                case INT3:
                                case INT4:
                                case INT8:
                                case DOUBLE:
                                case FLOAT: {
                                    objects[i] = row.getValue(i);
                                    break;
                                }
                                case NUMERIC: {
                                    Numeric value = (Numeric) row.getValue(i);
                                    if (value == null) {
                                        objects[i] = value;
                                    } else {
                                        objects[i] = value.bigDecimalValue();
                                    }
                                    break;
                                }
                                case VARSTRING:

                                case STRING: {
                                    Object rowValue = row.getValue(i);
                                    if (rowValue == null) {
                                        objects[i] = null;
                                    } else if (rowValue instanceof String) {

                                    } else if (rowValue instanceof Buffer) {
                                        objects[i] = ((Buffer) rowValue).getBytes();
                                    } else {
                                        objects[i] = rowValue.toString();
                                    }
                                    break;
                                }
                                case TINYBLOB:
                                case BLOB:
                                case MEDIUMBLOB:
                                case LONGBLOB: {
                                    Object rowValue = row.getValue(i);
                                    if (rowValue == null) {
                                        objects[i] = null;
                                    } else if (rowValue instanceof byte[]) {
                                        objects[i] = ((byte[]) rowValue);
                                    } else if (rowValue instanceof String) {
                                        objects[i] = rowValue.toString().getBytes(StandardCharsets.UTF_8);
                                    } else if (rowValue instanceof Buffer) {
                                        objects[i] = ((Buffer) rowValue).getBytes();
                                    } else {
                                        objects[i] = rowValue.toString().getBytes(StandardCharsets.UTF_8);
                                    }
                                    break;
                                }
                                case DATE: {
                                    Object rowValue = row.getValue(i);
                                    if (rowValue == null) {
                                        objects[i] = null;
                                    } else if (rowValue instanceof LocalDate) {
                                        objects[i] = rowValue;
                                    } else {
                                        throw new UnsupportedOperationException();
                                    }
                                    break;
                                }
                                case TIME: {
                                    Object rowValue = row.getValue(i);
                                    if (rowValue == null) {
                                        objects[i] = null;
                                    } else if (rowValue instanceof Duration) {
                                        objects[i] = rowValue;
                                    } else if (rowValue instanceof LocalTime) {
                                        LocalTime localTime = (LocalTime) rowValue;
                                        int hour = localTime.getHour();
                                        int minute = localTime.getMinute();
                                        int second = localTime.getSecond();
                                        int nano = localTime.getNano();
                                        objects[i] = Duration.ofHours(hour)
                                                .plusMinutes(minute)
                                                .plusMinutes(second)
                                                .plusNanos(nano);
                                    } else {
                                        throw new UnsupportedOperationException();
                                    }
                                    break;
                                }
                                case DATETIME:
                                    objects[i] = row.getValue(i);
                                case YEAR:
                                case TIMESTAMP:
                                case BIT: {
                                    objects[i] = row.getValue(i);
                                    break;
                                }
                                case UNBIND:
                                case JSON:
                                case GEOMETRY: {
                                    Object value = row.getValue(i);
                                    if (value == null) {
                                        objects[i] = null;
                                    } else {
                                        objects[i] = value.toString();
                                    }
                                    break;
                                }
                                case NULL:
                                    objects[i] = null;
                                    break;
                            }
                        }
                        collector.onRow(objects);
                    }
                    // Check for more ?
                    if (cursor.hasMore()) {
                        // Repeat the process...
                    } else {
                        collector.onComplete();
                        // No more rows - close the cursor
                        cursor.close();
                    }
                } else {
                    collector.onError(ar2.cause());
                }
            });
        }
    }

    @Override
    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        Future<PreparedStatement> preparedStatementFuture = mySQLConnection.prepare(sql);
        this.cursorHandler = new CursorHandler(collector);
        preparedStatementFuture.onSuccess(cursorHandler)
                .onFailure(throwable -> collector.onError(throwable));
    }

    public static MycatRowMetaData toColumnMetaData(List<ColumnDefinition> event) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        for (ColumnDefinition columnDescriptor : event) {
            if (columnDescriptor instanceof ColumnDefinition) {
                ColumnDefinition columnDefinition = (ColumnDefinition) columnDescriptor;
                String schemaName = columnDefinition.schema();
                String tableName = columnDefinition.orgTable();
                String columnName = columnDefinition.name();
                int columnType = columnDefinition.type().jdbcType.getVendorTypeNumber();
                int precision = 0;
                int scale = 0;
                String columnLabel = columnDefinition.name();
                boolean isAutoIncrement = false;
                boolean isCaseSensitive = false;
                boolean isNullable = (columnDefinition.flags() & ColumnDefinition.ColumnDefinitionFlags.NOT_NULL_FLAG) == 0;
                boolean isSigned = true;
                int displaySize = (int) columnDefinition.columnLength();
                resultSetBuilder.addColumnInfo(schemaName, tableName, columnName, columnType, precision, scale, columnLabel, isAutoIncrement, isCaseSensitive, isNullable,
                        isSigned, displaySize);
            } else {
                resultSetBuilder.addColumnInfo(columnDescriptor.name(),
                        columnDescriptor.jdbcType());
            }
        }
        RowBaseIterator build = resultSetBuilder.build();
        return build.getMetaData();
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params) {
        return Observable.create(new ObservableOnSubscribe<VectorSchemaRoot>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<VectorSchemaRoot> emitter) throws Throwable {
                Future<PreparedStatement> preparedStatementFuture = mySQLConnection.prepare(sql);
                preparedStatementFuture.onSuccess(new VectorCursorHandler(emitter,params))
                        .onFailure(throwable -> emitter.onError(throwable));
            }
        });
    }

    @Override
    public Future<List<Object>> call(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        return update(sql, params);
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        return update(sql, Collections.emptyList());
    }

    @Override
    public Future<SqlResult> update(String sql) {
        return update(sql, Collections.emptyList());
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        return this.mySQLConnection.preparedQuery(sql).execute(Tuple.tuple(params)).map(rows -> {
            int affectRows = rows.rowCount();
            long insertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID)).orElse(0L);
            return SqlResult.of(affectRows, insertId);
        });
    }

    @Override
    public Future<Void> close() {
        return mySQLConnection.close();
    }

    @Override
    public void abandonConnection() {
        mySQLConnection.resetConnection().onComplete(voidAsyncResult -> mySQLConnection.close());
    }

    @Override
    public Future<Void> abandonQuery() {
        if (cursorHandler != null) {
            return cursorHandler.close();
        } else {
            return Future.succeededFuture();
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(3306)
                .setHost("0.0.0.0")
                .setDatabase("mysql")
                .setUser("root")
                .setPassword("123456");

// Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5);

        for (int j = 0; j < 10; j++) {
            // Create the client pool
            MySQLPool client = (MySQLPoolImpl) MySQLPool.pool(connectOptions, poolOptions);
            Future<MySQLConnectionImpl> connectionFuture = (Future) client.getConnection();
            connectionFuture.onSuccess(new Handler<MySQLConnectionImpl>() {
                @Override
                public void handle(MySQLConnectionImpl mySQLConnection) {

                }
            });
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < 1; i++) {
                Future<io.vertx.sqlclient.RowSet<Row>> rowSetFuture = client
                        .query("SELECT 1")
                        .execute().onComplete(rowSetAsyncResult -> client.close());
                futures.add(rowSetFuture);
            }
            System.out.println("aaaaaaaaaaaaaaaaaaa");
            CompositeFuture.join(futures).toCompletionStage().toCompletableFuture().get();
            System.out.println("bbbbbbb");
            client.close().toCompletionStage().toCompletableFuture().get();
            System.out.println("cccccccccccccccccc");
// A simple query
        }


    }
}

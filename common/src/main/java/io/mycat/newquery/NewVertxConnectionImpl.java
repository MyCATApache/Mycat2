package io.mycat.newquery;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatMySQLRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLException;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.mysqlclient.impl.MySQLConnectionImpl;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.SneakyThrows;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class NewVertxConnectionImpl implements NewMycatConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewVertxConnectionImpl.class);

    MySQLConnectionImpl mySQLConnection;
    CursorHandler cursorHandler = null;

    public NewVertxConnectionImpl(MySQLConnectionImpl mySQLConnection) {
        this.mySQLConnection = mySQLConnection;
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        LOGGER.debug("sql:{}", sql);
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
            LOGGER.debug("close");
            if (cursor != null) {
                return cursor.close();
            }
            return Future.succeededFuture();
        }

        @Override
        public void handle(PreparedStatement preparedStatement) {

            // Create a cursor
            if (params.isEmpty()) {
                cursor = preparedStatement.cursor();
            } else {
                cursor = preparedStatement.cursor(Tuple.tuple(params));
            }
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
                            if (nullValue == null) {
                                if (valueVectors instanceof BaseFixedWidthVector) {
                                    ((BaseFixedWidthVector) valueVectors).setNull(rowId);
                                } else {
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
                                case INT8: {
                                    BaseIntVector baseIntVector = (BaseIntVector) valueVectors;
                                    baseIntVector.setWithPossibleTruncate(rowId, ((Number) nullValue).longValue());
                                    break;
                                }
                                case DOUBLE:
                                case FLOAT: {
                                    FloatingPointVector floatingPointVector = (FloatingPointVector) valueVectors;
                                    floatingPointVector.setWithPossibleTruncate(rowId, ((Number) nullValue).doubleValue());
                                    break;
                                }
                                case NUMERIC: {
                                    Numeric value = (Numeric) row.getValue(i);
                                    DecimalVector decimalVector = (DecimalVector) valueVectors;
                                    decimalVector.set(rowId, value.bigDecimalValue());
                                    break;
                                }
                                case VARSTRING:
                                case STRING: {
                                    VarCharVector varCharVector = (VarCharVector) valueVectors;
                                    Object value = row.getValue(i);
                                    if (value instanceof Buffer) {
                                        varCharVector.setSafe(rowId, ((Buffer) value).getBytes());
                                    } else if (value instanceof String) {
                                        varCharVector.setSafe(rowId, ((String) value).getBytes());
                                    } else {
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
                                    varBinaryVector.set(rowId, buffer.getBytes());
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
        private String sql;
        List<Object> params;
        PreparedStatement preparedStatement;
        public CursorHandler(MysqlCollector collector, String sql, List<Object> params) {
            this.collector = collector;
            this.sql = sql;
            this.params = params;
        }

        public synchronized Future<Void> close() {
            LOGGER.debug("close");
            if (cursor != null) {
                try {
                    return cursor.close();
                } finally {
                    cursor = null;
                }
            }
            if (preparedStatement!=null){
                preparedStatement.close();
                preparedStatement = null;
            }
            return Future.succeededFuture();
        }

        @Override
        public void handle(PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
            try {
                // Create a cursor
                if (params.isEmpty()) {
                    cursor = preparedStatement.cursor();
                } else {
                    cursor = preparedStatement.cursor(Tuple.tuple(params));
                }

                int batch = 8192;
                // Read 50 rows
                cursor.read(batch, new Handler<AsyncResult<io.vertx.sqlclient.RowSet<Row>>>() {
                    @Override
                    public void handle(AsyncResult<io.vertx.sqlclient.RowSet<Row>> ar2) {
                        try {
                            if (ar2.succeeded()) {
                                io.vertx.sqlclient.RowSet<Row> rows = ar2.result();
                                boolean end = batch > rows.size();
                                if (columnDescriptors == null) {
                                    columnDescriptors = (List) rows.columnDescriptors();
                                    collector.onColumnDef(toColumnMetaData(columnDescriptors));
                                }
                                int size = columnDescriptors.size();
                                for (Row row : rows) {
                                    Object[] objects = normailize(columnDescriptors, row);
                                    collector.onRow(objects);
                                }
                                // Check for more ?
                                if (!end && cursor.hasMore()) {
                                    // Repeat the process...
                                } else {
                                    // No more rows - close the cursor
                                    Future<Void> closeFuture = cursor.close();
                                    cursor = null;
                                    closeFuture
                                            .onComplete(voidAsyncResult -> collector.onComplete());
                                }
                            } else {
                                closeCursor()
                                        .onComplete(voidAsyncResult -> collector.onError(ar2.cause()));

                            }
                        } catch (Throwable throwable) {
                            closeCursor()
                                    .onComplete(voidAsyncResult -> collector.onError(ar2.cause()));

                        }
                    }
                });
            } catch (Throwable throwable) {
                closeCursor()
                        .onComplete(voidAsyncResult -> collector.onError(throwable));
            }
        }

        private Future<Void> closeCursor() {
            if (cursor != null) {
                try {
                    return cursor.close();

                } finally {
                    cursor = null;
                }
            }
            if (preparedStatement!=null){
                preparedStatement.close();
                preparedStatement = null;
            }
            return Future.succeededFuture();
        }
    }

    @Override
    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        LOGGER.debug("sql:{}", sql);
        Future<io.vertx.sqlclient.RowSet<Row>> execute = mySQLConnection.query(sql).execute();
        execute=  execute.onFailure(new Handler<Throwable>() {
            @Override
            public void handle(Throwable event) {
                collector.onError(event);
            }
        });
        execute.onSuccess(new Handler<io.vertx.sqlclient.RowSet<Row>>() {
            @Override
            public void handle(io.vertx.sqlclient.RowSet<Row> event) {
                try {
                    List<ColumnDefinition> columnDescriptors = (List) event.columnDescriptors();
                    MycatRowMetaData mycatRowMetaData = toColumnMetaData(columnDescriptors);
                    collector.onColumnDef(mycatRowMetaData);
                    int columnCount = mycatRowMetaData.getColumnCount();
                    RowIterator<Row> iterator = event.iterator();
                    while (iterator.hasNext()) {
                        Row next = iterator.next();
                        Object[] objects = new Object[next.size()];
                        for (int i = 0; i < columnCount; i++) {
                            objects[i] = next.getValue(i);
                        }
                        collector.onRow(objects);
                    }
                    collector.onComplete();
                }catch (Exception e){
                    collector.onError(e);
                }

            }
        });
    }

    public static MycatRowMetaData toColumnMetaData(List<ColumnDefinition> event) {
        boolean isMysql = event.get(0) instanceof ColumnDefinition;
        if (isMysql) {
            List<ColumnDefinition> columnDefinitions = event;
            List<ColumnDefPacket> columnDefPackets = new ArrayList<>(event.size());

            for (ColumnDefinition columnDefinition : columnDefinitions) {
                final String catalog = columnDefinition.catalog();
                final String schema = columnDefinition.schema();
                final String table = columnDefinition.table();
                final String orgTable = columnDefinition.orgTable();
                final String name = columnDefinition.name();
                final String orgName = columnDefinition.orgName();
                final int characterSet = columnDefinition.characterSet();
                final long columnLength = columnDefinition.columnLength();
                final DataType type = columnDefinition.type();
                final int flags = columnDefinition.flags();
                byte decimals = columnDefinition.decimals();
                if (decimals == 31) {
                    decimals = 0;
                }
                ColumnDefPacketImpl mySQLFieldInfo = new ColumnDefPacketImpl();

                mySQLFieldInfo.setColumnCatalog(catalog.getBytes());
                mySQLFieldInfo.setColumnSchema(schema.getBytes());
                mySQLFieldInfo.setColumnTable(table.getBytes());
                mySQLFieldInfo.setColumnOrgTable(orgTable.getBytes());
                mySQLFieldInfo.setColumnName(name.getBytes());
                mySQLFieldInfo.setColumnOrgName(orgName.getBytes());
                mySQLFieldInfo.setColumnCharsetSet(characterSet);
                mySQLFieldInfo.setColumnLength((int) columnLength);
                mySQLFieldInfo.setColumnType(type.id);

                mySQLFieldInfo.setColumnFlags(flags);

                mySQLFieldInfo.setColumnDecimals(decimals);

                columnDefPackets.add(mySQLFieldInfo);
            }
            return new MycatMySQLRowMetaData(columnDefPackets);
        } else {
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            for (ColumnDefinition columnDescriptor : event) {
                resultSetBuilder.addColumnInfo(columnDescriptor.name(),
                        columnDescriptor.jdbcType());
            }
            RowBaseIterator build = resultSetBuilder.build();
            return build.getMetaData();
        }
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return Observable.create(new ObservableOnSubscribe<VectorSchemaRoot>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<VectorSchemaRoot> emitter) throws Throwable {
                Future<PreparedStatement> preparedStatementFuture = mySQLConnection.prepare(sql);
                preparedStatementFuture.onSuccess(new VectorCursorHandler(emitter, params))
                        .onFailure(throwable -> emitter.onError(mapException(throwable)))
                        .onComplete(event -> preparedStatementFuture.onSuccess(event1 -> event1.close()));
            }
        });
    }

    @Override
    public Future<List<Object>> call(String sql) {
        Query<io.vertx.sqlclient.RowSet<Row>> query = mySQLConnection.query(sql);
        Future<List<Object>> listFuture = query.execute().map(rowRowSet -> {
            ArrayList<Object> resList = new ArrayList<>();

            for (; ; ) {
                if (rowRowSet.rowCount() == 0 && !rowRowSet.columnsNames().isEmpty()) {
                    RowIterator<Row> rowIterator = rowRowSet.iterator();
                    List<ColumnDefinition> columnDescriptors = (List) rowRowSet.columnDescriptors();
                    MycatRowMetaData mycatRowMetaData = toColumnMetaData(columnDescriptors);
                    List<Object[]> rows = new LinkedList<>();
                    while (rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        rows.add(normailize(columnDescriptors, row));
                    }
                    resList.add(new RowSet(mycatRowMetaData, rows));
                } else {
                    resList.add(new long[]{rowRowSet.rowCount(), rowRowSet.property(MySQLClient.LAST_INSERTED_ID)});
                }
                rowRowSet = rowRowSet.next();
                if (rowRowSet == null || rowRowSet.rowCount() == 0 && rowRowSet.size() == 0) {
                    break;
                }
            }
            return resList;
        });
        return mapException(listFuture);
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
        LOGGER.debug("sql:{}", sql);
        if (!sql.startsWith("begin") && !sql.startsWith("XA")) {
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
        }

        Query<io.vertx.sqlclient.RowSet<Row>> preparedStatementFuture = this.mySQLConnection.query(sql);
        Future<SqlResult> sqlResultFuture = preparedStatementFuture.execute().map(rows -> {
            int affectRows = rows.rowCount();
            long insertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID)).orElse(0L);
            return SqlResult.of(affectRows, insertId);
        });
        return mapException(sqlResultFuture);
    }

    private static <T> Future<T> mapException(Future<T> sqlResultFuture) {
        return sqlResultFuture.transform(sqlResultAsyncResult -> {
            if (sqlResultAsyncResult.succeeded()) {
                return Future.succeededFuture(sqlResultAsyncResult.result());
            }
            Throwable throwable = sqlResultAsyncResult.cause();
            throwable = mapException(throwable);
            return Future.failedFuture(throwable);
        });
    }

    private static Throwable mapException(Throwable throwable) {
        if (throwable instanceof MySQLException) {
            MySQLException mySQLException = (MySQLException) throwable;
            int errorCode = mySQLException.getErrorCode();
            String sqlState = mySQLException.getSqlState();
            String message = mySQLException.getMessage();
            throwable = new SQLException(message, sqlState, errorCode, throwable);
        }
        return throwable;
    }

    @Override
    public Future<Void> close() {
        LOGGER.debug("close");
        synchronized (NewVertxConnectionImpl.this) {
            if (mySQLConnection != null) {
                mySQLConnection.close();
                mySQLConnection = null;
            }
        }
        return Future.succeededFuture();
    }

    @Override
    public void abandonConnection() {
        LOGGER.debug("abandonConnection");
        Future<Void> abandonQuery = abandonQuery();
        abandonQuery.onComplete(unused -> mySQLConnection.resetConnection()
                .onComplete(voidAsyncResult -> {
                    synchronized (NewVertxConnectionImpl.this) {
                        if (mySQLConnection != null) {
                            mySQLConnection.close();
                            mySQLConnection = null;
                        }
                    }

                }));

    }

    @Override
    public Future<Void> abandonQuery() {
        LOGGER.debug("abandonQuery");
        if (cursorHandler != null) {
            Future<Void> closeFuture = cursorHandler.close();
            cursorHandler = null;
            return closeFuture;
        } else {
            return Future.succeededFuture();
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(3306)
                .setHost("localhost")
                .setDatabase("mysql")
                .setUser("root")
                .setPassword("123456");

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(1);
        MySQLPool client = MySQLPool.pool(connectOptions, poolOptions);

        Future<SqlConnection> connectionFuture = client.getConnection();
        PreparedStatement preparedStatement1 = connectionFuture.flatMap(connection -> connection.prepare("SELECT  1")).toCompletionStage().toCompletableFuture().get();
        Cursor cursor = preparedStatement1.cursor();
        io.vertx.sqlclient.RowSet<Row> rows = cursor.read(8192).toCompletionStage().toCompletableFuture().get();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        cursor.close().onComplete(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> voidAsyncResult) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        System.out.println();
//        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
//                .setPort(3306)
//                .setHost("0.0.0.0")
//                .setDatabase("mysql")
//                .setUser("root")
//                .setPassword("123456");
//
//// Pool options
//        PoolOptions poolOptions = new PoolOptions()
//                .setMaxSize(5);
//
//        for (int j = 0; j < 10; j++) {
//            // Create the client pool
//            MySQLPool client = (MySQLPoolImpl) MySQLPool.pool(connectOptions, poolOptions);
//            Future<MySQLConnectionImpl> connectionFuture = (Future) client.getConnection();
//            connectionFuture.onSuccess(new Handler<MySQLConnectionImpl>() {
//                @Override
//                public void handle(MySQLConnectionImpl mySQLConnection) {
//
//                }
//            });
//            List<Future> futures = new ArrayList<>();
//            for (int i = 0; i < 1; i++) {
//                Future<io.vertx.sqlclient.RowSet<Row>> rowSetFuture = client
//                        .query("SELECT 1")
//                        .execute().onComplete(rowSetAsyncResult -> client.close());
//                futures.add(rowSetFuture);
//            }
//            System.out.println("aaaaaaaaaaaaaaaaaaa");
//            CompositeFuture.join(futures).toCompletionStage().toCompletableFuture().get();
//            System.out.println("bbbbbbb");
//            client.close().toCompletionStage().toCompletableFuture().get();
//            System.out.println("cccccccccccccccccc");
// A simple query
    }

    @NotNull
    private static Object[] normailize(List<ColumnDefinition> columnDescriptors, Row row) {
        Object[] objects = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
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
                        objects[i] = rowValue;
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
                    break;
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
        return objects;
    }


}

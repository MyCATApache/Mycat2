//package io.mycat.newquery;
//
//import io.mycat.beans.mycat.MycatDataType;
//import io.mycat.beans.mycat.MycatField;
//import io.mycat.beans.mycat.MycatMySQLRowMetaData;
//import io.mycat.beans.mycat.MycatRowMetaData;
//import io.mycat.beans.mysql.MySQLFieldsType;
//import io.mycat.beans.mysql.packet.ColumnDefPacket;
//import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
//import io.mycat.util.JavaClassToMySQLTypeUtil;
//import io.r2dbc.spi.*;
//import io.reactivex.rxjava3.annotations.NonNull;
//import io.reactivex.rxjava3.core.Observable;
//import io.reactivex.rxjava3.core.Observer;
//import io.reactivex.rxjava3.disposables.Disposable;
//import io.reactivex.rxjava3.functions.Consumer;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.Promise;
//import org.apache.arrow.vector.VectorSchemaRoot;
//import org.mariadb.r2dbc.api.MariadbConnection;
//import org.mariadb.r2dbc.codec.DataType;
//import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
//
//import java.nio.ByteBuffer;
//import java.sql.JDBCType;
//import java.time.Duration;
//import java.time.LocalTime;
//import java.util.*;
//import java.util.function.BiFunction;
//
//public class NewMariadbConnectionImpl implements NewMycatConnection {
//    final MariadbConnection connection;
//    public Disposable disposable;
//
//    public NewMariadbConnectionImpl(MariadbConnection connection) {
//        this.connection = connection;
//    }
//
//    @Override
//    public Future<RowSet> query(String sql, List<Object> params) {
//        Future<RowSet> future = Future.future(new Handler<Promise<RowSet>>() {
//            @Override
//            public void handle(Promise<RowSet> rowSetPromise) {
//                prepareQuery(sql, params, new MysqlCollector() {
//                    MycatRowMetaData mycatRowMetaData;
//                    ArrayList<Object[]> objects = new ArrayList<>();
//
//                    @Override
//                    public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
//                        this.mycatRowMetaData = mycatRowMetaData;
//                    }
//
//                    @Override
//                    public void onRow(Object[] row) {
//                        objects.add(row);
//                    }
//
//                    @Override
//                    public void onComplete() {
//                        RowSet rowSet = new RowSet(mycatRowMetaData, objects);
//                        rowSetPromise.tryComplete(rowSet);
//                    }
//
//                    @Override
//                    public void onError(Throwable e) {
//                        rowSetPromise.fail(e);
//                    }
//                });
//            }
//        });
//
//        return future;
//    }
//
//    @Override
//    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
//        Statement statement = connection.createStatement(sql);
//        for (int i = 0; i < params.size(); i++) {
//            statement.bind(i, params.get(i));
//        }
//        Observable.fromPublisher(statement.execute()).subscribe(new Observer<Result>() {
//            @Override
//            public void onSubscribe(@NonNull Disposable d) {
//                disposable = d;
//            }
//
//            @Override
//            public void onNext(@NonNull Result result) {
//                result.map(new BiFunction<Row, RowMetadata, Object>() {
//                    MycatMySQLRowMetaData mycatMySQLRowMetaData;
//                    List<MycatField> mycatFields = new ArrayList<>();
//
//                    @Override
//                    public Object apply(Row row, RowMetadata rowMetadata) {
//                        if (mycatMySQLRowMetaData == null) {
//                            initMeta(rowMetadata);
//                            collector.onColumnDef(mycatMySQLRowMetaData);
//                        }
//                        Object[] objects = new Object[mycatFields.size()];
//                        for (int i = 0; i < mycatFields.size(); i++) {
//                            MycatField mycatField = mycatFields.get(i);
//                            Object o = row.get(i);
//                            objects[i] = mycatField.getMycatDataType().fromValue(o);
//                        }
//                        collector.onRow(objects);
//                        return null;
//                    }
//
//                    private void initMeta(RowMetadata rowMetadata) {
//                        if (mycatMySQLRowMetaData == null) {
//                            mycatFields = new ArrayList<>();
//                            List<ColumnDefPacket> columnDefPackets = new ArrayList<>();
//                            for (ColumnMetadata columnMetadata : rowMetadata.getColumnMetadatas()) {
//                                ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
//
//
//                                String name = columnMetadata.getName();
//                                final Class<?> javaClass = columnMetadata.getJavaType();
//                                Nullability nullability = columnMetadata.getNullability();
//                                Integer scale = Optional.ofNullable(columnMetadata.getScale()).orElse(0);
//                                Integer precision = Optional.ofNullable(columnMetadata.getPrecision()).orElse(0);
//                                Object nativeTypeMetadata = columnMetadata.getNativeTypeMetadata();
//                                boolean nullable = !(nullability != null && nullability == Nullability.NON_NULL);
//
//                                final Class matchJavaClass;
//                                if (javaClass == ByteBuffer.class) {
//                                    matchJavaClass = byte[].class;
//                                } else if (javaClass == LocalTime.class) {
//                                    matchJavaClass = Duration.class;
//                                } else if (javaClass == BitSet.class) {
//                                    matchJavaClass = Long.class;
//                                } else if (javaClass == Blob.class) {
//                                    matchJavaClass = byte[].class;
//                                } else {
//                                    matchJavaClass = javaClass;
//                                }
//                                MycatDataType mycatDataType = Arrays.stream(MycatDataType.values()).filter(c -> c.getJavaClass() == matchJavaClass).findFirst().orElse(MycatDataType.VARCHAR);
//
//                                if (nativeTypeMetadata != null && nativeTypeMetadata.getClass().getCanonicalName().contains("ColumnDefinitionPacket")) {
//
//                                    ColumnDefinitionPacket columnDefinitionPacket = (ColumnDefinitionPacket) nativeTypeMetadata;
//                                    int charset = columnDefinitionPacket.getCharset();
//                                    int length = (int) columnDefinitionPacket.getLength();
//                                    DataType dataType = columnDefinitionPacket.getType();
//                                    byte decimals = columnDefinitionPacket.getDecimals();
//                                    boolean binary = columnDefinitionPacket.isBinary();
//                                    boolean blob = columnDefinitionPacket.isBlob();
//                                    boolean signed = columnDefinitionPacket.isSigned();
//                                    String table = columnDefinitionPacket.getTable();
//                                    String tableAlias = columnDefinitionPacket.getTableAlias();
//                                    String columnAlias = columnDefinitionPacket.getColumnAlias();
//
//                                    columnDefPacket.setColumnName(columnAlias.getBytes());
//                                    columnDefPacket.setColumnOrgName(columnAlias.getBytes());
//                                    columnDefPacket.setColumnTable(tableAlias.getBytes());
//                                    columnDefPacket.setColumnOrgTable(table.getBytes());
//                                    columnDefPacket.setColumnType(columnDefinitionPacket.getType().get());
//                                    columnDefPacket.setColumnLength(length);
//                                    columnDefPacket.setColumnDecimals(decimals);
//                                    columnDefPacket.setColumnCharsetSet(charset);
//                                    int flag = 0;
//                                    if (binary) {
//                                        columnDefPacket.setColumnCharsetSet(63);
//                                    }
//                                    if (blob) {
//                                        flag |= MySQLFieldsType.BLOB_FLAG;
//                                    }
//                                    if (!signed) {
//                                        flag |= MySQLFieldsType.UNSIGNED_FLAG;
//                                    }
//                                    if (nullability != null && nullability == Nullability.NON_NULL) {
//                                        flag |= MySQLFieldsType.NOT_NULL_FLAG;
//                                    }
//                                    if (dataType == DataType.TIMESTAMP) {
//                                        flag |= MySQLFieldsType.TIMESTAMP_FLAG;
//                                    }
//                                    columnDefPacket.setColumnFlags(flag);
//                                    mycatFields.add(MycatField.of(name, mycatDataType, nullable, scale, precision));
//                                } else {
//                                    mycatFields.add(MycatField.of(name, mycatDataType, nullable, scale, precision));
//                                    JDBCType jdbcType = mycatDataType.getSignedJdbcType();
//                                    int mySQLType = JavaClassToMySQLTypeUtil.getMySQLType(javaClass);
//                                    columnDefPacket.setColumnName(name.getBytes());
//                                    columnDefPacket.setColumnOrgName(name.getBytes());
//                                    columnDefPacket.setColumnType(mySQLType);
//                                    columnDefPacket.setColumnLength(255);
//                                    columnDefPacket.setColumnDecimals((byte) scale.byteValue());
//                                    columnDefPacket.setColumnType(MySQLFieldsType.fromJdbcType(jdbcType.getVendorTypeNumber()));
//
//                                    int flag = 0;
//                                    if (jdbcType == JDBCType.TIMESTAMP || jdbcType == JDBCType.TIMESTAMP_WITH_TIMEZONE) {
//                                        flag |= MySQLFieldsType.TIMESTAMP_FLAG;
//                                    } else if (jdbcType == JDBCType.BINARY) {
//                                        columnDefPacket.setColumnCharsetSet(63);
//                                    } else if (jdbcType == JDBCType.BLOB) {
//                                        flag |= MySQLFieldsType.BLOB_FLAG;
//                                    }
//                                    if (!nullable) {
//                                        flag |= MySQLFieldsType.NOT_NULL_FLAG;
//                                    }
//                                    columnDefPacket.setColumnType(flag);
//                                }
//                                columnDefPackets.add(columnDefPacket);
//                            }
//                            mycatMySQLRowMetaData = new MycatMySQLRowMetaData(columnDefPackets);
//                        }
//                    }
//                });
//            }
//
//            @Override
//            public void onError(@NonNull Throwable e) {
//                collector.onError(e);
//            }
//
//            @Override
//            public void onComplete() {
//                collector.onComplete();
//            }
//        });
//    }
//
//
//    @Override
//    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params) {
//     throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Future<List<Object>> call(String sql) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public Future<SqlResult> insert(String sql, List<Object> params) {
//        return update(sql,params);
//    }
//
//    @Override
//    public Future<SqlResult> insert(String sql) {
//        return insert(sql, Collections.emptyList());
//    }
//
//    @Override
//    public Future<SqlResult> update(String sql) {
//        return update(sql,Collections.emptyList());
//    }
//
//    @Override
//    public Future<SqlResult> update(String sql, List<Object> params) {
//        return Future.future(promise -> {
//            SqlResult sqlResult = new SqlResult();
//            Statement statement = connection.createStatement(sql);
//            for (int i = 0; i <params.size(); i++) {
//                statement.bind(i, params.get(i));
//            }
//            Observable<? extends Result> observable = Observable.fromPublisher(statement.execute());
//            observable.doOnError(throwable -> promise.tryFail(throwable));
//            observable.forEach((Consumer<Result>) result -> Observable
//                    .fromPublisher(result.getRowsUpdated()).doOnError(throwable -> promise.tryFail(throwable)).forEach(integer -> {
//                        sqlResult.setAffectRows(integer);
//                        result.map((row, rowMetadata) -> {
//                            Number o = (Number) row.get(0);
//                            sqlResult.setLastInsertId(o.longValue());
//                            return null;
//                        });
//                        promise.tryComplete(sqlResult);
//                    }));
//        });
//    }
//
//    @Override
//    public Future<Void> close() {
//        connection.close().block();
//        return Future.succeededFuture();
//    }
//
//    @Override
//    public void abandonConnection() {
//        connection.close();
//    }
//
//    @Override
//    public synchronized Future<Void> abandonQuery() {
//        if (disposable != null) {
//            disposable.dispose();
//            disposable = null;
//        }
//        return Future.succeededFuture();
//    }
//}

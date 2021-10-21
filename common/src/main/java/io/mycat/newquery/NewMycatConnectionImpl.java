package io.mycat.newquery;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.jdbcclient.impl.actions.JDBCResponse;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NewMycatConnectionImpl implements NewMycatConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewMycatConnectionImpl.class);

    boolean needLastInsertId;
    Connection connection;
    ResultSet resultSet;

    public NewMycatConnectionImpl(boolean needLastInsertId, Connection connection) {
        this.needLastInsertId = needLastInsertId;
        this.connection = connection;
    }

    public NewMycatConnectionImpl(Connection connection) {
        this.connection = connection;
        this.needLastInsertId = true;
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
                    public void onError(Exception e) {
                        rowSetPromise.fail(e);
                    }
                });
            }
        });

        return future;
    }

    @Override
    public synchronized void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        try {
            if (params.isEmpty()) {
                try (Statement statement = connection.createStatement();) {
                    onSend();
                    resultSet = statement.executeQuery(sql);
                    onRev();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    collector.onColumnDef(new CopyMycatRowMetaData(new JdbcRowMetaData(metaData)));
                    int columnLimit = columnCount + 1;
                    while (!resultSet.isClosed() && resultSet.next()) {
                        Object[] objects = new Object[columnCount];
                        for (int i = 1, j = 0; i < columnLimit; i++, j++) {
                            objects[j] = resultSet.getObject(i);
                        }
                        collector.onRow(objects);
                    }
                }
            } else {
                try (PreparedStatement statement = connection.prepareStatement(sql);) {
                    int limit = params.size() + 1;
                    for (int i = 1; i < limit; i++) {
                        statement.setObject(i, params.get(i - 1));
                    }
                    onSend();
                    resultSet = statement.executeQuery();
                    onRev();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    collector.onColumnDef(new CopyMycatRowMetaData(new JdbcRowMetaData(metaData)));
                    int columnLimit = columnCount + 1;
                    while (!resultSet.isClosed() && resultSet.next()) {
                        Object[] objects = new Object[columnCount];
                        for (int i = 1, j = 0; i < columnLimit; i++, j++) {
                            objects[j] = resultSet.getObject(i);
                        }
                        collector.onRow(objects);
                    }
                }
            }

        } catch (Exception e) {
            collector.onError(e);
        } finally {
            collector.onComplete();
            resultSet = null;
        }
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params) {
        return Observable.create(new ObservableOnSubscribe<VectorSchemaRoot>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<VectorSchemaRoot> emitter) throws Throwable {


                synchronized (NewMycatConnectionImpl.this) {


                    JdbcToArrowConfigBuilder jdbcToArrowConfigBuilder = new JdbcToArrowConfigBuilder();
                    JdbcToArrowConfig jdbcToArrowConfig = jdbcToArrowConfigBuilder.build();
                    try {
                        if (params.isEmpty()) {
                            try (Statement statement = connection.createStatement();) {
                                onSend();
                                resultSet = statement.executeQuery(sql);
                                onRev();
                                try (ArrowVectorIterator arrowVectorIterator = ArrowVectorIterator.create(resultSet, jdbcToArrowConfig)) {
                                    while (arrowVectorIterator.hasNext()) {
                                        VectorSchemaRoot schemaRoot = arrowVectorIterator.next();
                                        emitter.onNext(schemaRoot);
                                    }
                                }
                            }
                        } else {
                            try (PreparedStatement statement = connection.prepareStatement(sql);) {
                                int limit = params.size() + 1;
                                for (int i = 1; i < limit; i++) {
                                    statement.setObject(i, params.get(i - 1));
                                }
                                onSend();
                                resultSet = statement.executeQuery();
                                onRev();
                                try (ArrowVectorIterator arrowVectorIterator = ArrowVectorIterator.create(resultSet, jdbcToArrowConfig)) {
                                    while (arrowVectorIterator.hasNext()) {
                                        VectorSchemaRoot schemaRoot = arrowVectorIterator.next();
                                        emitter.onNext(schemaRoot);
                                    }
                                }
                            }
                        }

                    } catch (Exception e) {
                        emitter.onError(e);
                    } finally {
                        emitter.onComplete();
                        resultSet = null;
                    }


                }
            }
        });
    }

    @Override
    public synchronized Future<List<Object>> call(String sql) {
        try {
            ArrayList<Object> resultSetList = new ArrayList<>();
            CallableStatement callableStatement = connection.prepareCall(sql);
            boolean firstExecuteRes = callableStatement.execute();
            int updateCount = callableStatement.getUpdateCount();
            if (firstExecuteRes){
                ResultSet resultSet = callableStatement.getResultSet();//获取第一个resultSet
                MycatRowMetaData metaData = new CopyMycatRowMetaData( new JdbcRowMetaData(resultSet.getMetaData()));
                List<Object[]> objects = new ArrayList<>();
                while (resultSet.next()){
                    int columnCount = metaData.getColumnCount();
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = resultSet.getObject(i+1);
                    }
                    objects.add(row);
                }
                RowSet rowSet = new RowSet(metaData, objects);
                resultSetList.add(rowSet);
            }else {
                resultSetList.add(new long[]{updateCount,0});
            }
            return Future.succeededFuture(resultSetList);
        } catch (Throwable throwable) {
            return Future.failedFuture(throwable);
        }
    }

    @Override
    public synchronized Future<SqlResult> insert(String sql, List<Object> params) {
        try {
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
            SqlResult sqlResult = new SqlResult();
            sqlResult.setAffectRows(affectRows);
            sqlResult.setLastInsertId(lastInsertId);
            return Future.succeededFuture(sqlResult);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    @Override
    public synchronized Future<SqlResult> insert(String sql) {
        return insert(sql, Collections.emptyList());
    }

    @Override
    public synchronized Future<SqlResult> update(String sql) {
        return update(sql, Collections.emptyList());
    }

    @Override
    public synchronized Future<SqlResult> update(String sql, List<Object> params) {
        try {
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
            return Future.failedFuture(e);
        }
    }

    @Override
    public Future<Void> close() {
        JdbcUtils.close(connection);
        return Future.succeededFuture();
    }

    @Override
    public void abandonConnection() {
        if (this.connection instanceof DruidPooledConnection) {
            DruidPooledConnection connection = (DruidPooledConnection) this.connection;
            JdbcUtils.close(connection.getConnection());
            connection.abandond();
            JdbcUtils.close(connection);
        } else {
            JdbcUtils.close(this.connection);
        }
    }

    @Override
    public Future<Void> abandonQuery() {
        if (resultSet != null) {
            JdbcUtils.close(resultSet);
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
                needLastInsertId = false;
            }
        }
        return lastInsertId;
    }
}

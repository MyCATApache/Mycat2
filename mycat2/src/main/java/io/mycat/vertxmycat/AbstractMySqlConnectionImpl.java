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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.resultset.TextConvertorImpl;
import io.mycat.util.VertxUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.sqlclient.*;
import org.apache.calcite.avatica.util.ByteString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class AbstractMySqlConnectionImpl extends AbstractMySqlConnection {
   public static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySqlConnectionImpl.class);
    volatile Handler<Throwable> exceptionHandler;
    volatile Handler<Void> closeHandler;
    volatile MySQLClientSession mySQLClientSession;

    public AbstractMySqlConnectionImpl(MySQLClientSession mySQLClientSession) {
        this.mySQLClientSession = mySQLClientSession;
    }

    @Override
    public Future<PreparedStatement> prepare(String sql) {
        return Future.succeededFuture(new MycatVertxPreparedStatement(sql, this));
    }

    @Override
    public SqlConnection prepare(String s, PrepareOptions prepareOptions, Handler<AsyncResult<PreparedStatement>> handler) {
        return null;
    }

    @Override
    public Future<PreparedStatement> prepare(String s, PrepareOptions prepareOptions) {
        return null;
    }

    @Override
    public MySQLConnection exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    @Override
    public MySQLConnection closeHandler(Handler<Void> handler) {
        closeHandler = handler;
        return this;
    }

    @Override
    public Future<Void> ping() {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_PING, new byte[]{},
                        commandResponse(promise));
        return promise.future();
    }


    @Override
    public Future<Void> specifySchema(String schemaName) {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_INIT_DB, schemaName.getBytes(StandardCharsets.UTF_8),
                        commandResponse(promise));
        return promise.future();
    }


    @Override
    public Future<Void> resetConnection() {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_RESET_CONNECTION, new byte[]{},
                        commandResponse(promise));
        return promise.future();
    }

    @Override
    public Query<RowSet<Row>> query(String sql) {
        return new RowSetQuery(sql, this);
    }

    public static String apply(String parameterizedSql, List<Object> parameters) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                parent.replace(x, fromJavaObject(parameters.get(x.getIndex())));
            }
        });
        return sqlStatement.toString();
    }

    private static SQLExpr fromJavaObject(Object o) {
        if (o == null) {
            return new SQLNullExpr();
        }
        o = adaptType(o);

        if (o instanceof String) {
            return new SQLCharExpr((String) o);
        }
        if (o instanceof Boolean) {
            o = (Boolean) o ? 1 : 0;
        }

//        if (o instanceof BigDecimal) {
//            return new SQLDecimalExpr((BigDecimal) o);
//        }

        if (o instanceof Byte || o instanceof Short || o instanceof Integer || o instanceof Long || o instanceof BigInteger) {
            return new SQLIntegerExpr((Number) o);
        }

        if (o instanceof Number) {
            return new SQLNumberExpr((Number) o);
        }

        if (o instanceof Date) {
            return new SQLTimestampExpr((Date) o, null);
        }
        if (o instanceof byte[]) {
            o = new ByteString((byte[]) o);
        }
        if (o instanceof ByteString) {
            return new SQLHexExpr(((ByteString) o).toString(16));
        }
        throw new UnsupportedOperationException("unsupport type:" + o.getClass());
    }

    public static Object adaptType(Object value) {
        // we must convert types (to comply to JDBC)

        if (value instanceof LocalTime) {
            // -> java.sql.Time
            LocalTime time = (LocalTime) value;
            return Time.valueOf(time);
        } else if (value instanceof LocalDate) {
            // -> java.sql.Date
            LocalDate date = (LocalDate) value;
            return java.sql.Date.valueOf(date);
        } else if (value instanceof Instant) {
            // -> java.sql.Timestamp
            Instant timestamp = (Instant) value;
            return Timestamp.from(timestamp);
        } else if (value instanceof Buffer) {
            // -> java.sql.Blob
            Buffer blob = (Buffer) value;
            return blob.getBytes();
        } else if (value instanceof ByteString) {
            // -> java.sql.Blob
            return ((ByteString) value).getBytes();
        } else if (value instanceof Duration) {
            // -> java.sql.Blob
            Duration duration = (Duration) value;
            return Time.valueOf(LocalTime.parse(TextConvertorImpl.toString(duration)));
        }

        return value;
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
        return new RowSetMySqlPreparedTextQuery(sql, this);
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String s, PrepareOptions prepareOptions) {
        return null;
    }

    @NotNull
    public static List<Object> toObjects(Tuple tuple) {
        if (tuple.size() == 0){
            return Collections.emptyList();
        }
        int size = tuple.size();
        ArrayList<Object> objects = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            objects.add(tuple.getValue(i));
        }
        return objects;
    }


    @Override
    public Future<Void> close() {
        if (mySQLClientSession == null) {
            return VertxUtil.newSuccessPromise();
        } else {
            Promise<Void> promise = Promise.promise();
            PromiseInternal<Void> close = mySQLClientSession.close(true, "close");
            mySQLClientSession = null;
            close.onComplete(event -> {
                if (closeHandler != null) {
                    try {
                        closeHandler.handle(null);
                    } catch (Throwable throwable) {
                        if (exceptionHandler != null) {
                            exceptionHandler.handle(throwable);
                        }
                    }
                }
                promise.tryComplete();
            });
            return promise.future();
        }
    }

    @NotNull
    private ResultSetCallBack<MySQLClientSession> commandResponse(Promise<Void> promise) {
        return new CommandResponse(promise);
    }

}

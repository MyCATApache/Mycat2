package io.mycat.vertx;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.MycatValueFactory;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.data.Numeric;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import org.apache.calcite.rel.type.RelDataType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.List;

import static java.sql.Types.*;

public class  BaseRowObservable extends RowObservable implements StreamMysqlCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRowObservable.class);
    protected MycatRowMetaData metaData;
    protected Observer<? super Object[]> observer;
    protected final Future<SqlConnection> sqlConnectionFuture;
    protected final String sql;
    protected final List<Object> values;
    protected ColumnDefinition[] columnDefinitions;

    public BaseRowObservable(Future<SqlConnection> sqlConnectionFuture,
                             String sql,
                             List<Object> values,
                             MycatRowMetaData rowMetaData) {
        this.sqlConnectionFuture = sqlConnectionFuture;
        this.sql = sql;
        this.values = values;
        this.metaData = rowMetaData;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
        this.observer = observer;
        sqlConnectionFuture
                .flatMap(connection -> connection.prepare(sql)).compose(preparedStatement -> {
            PreparedQuery<RowSet<Row>> query = preparedStatement.query();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RowObservableImpl sql:{} connection:{}", sql, sqlConnectionFuture.result());
            }
            PreparedQuery<SqlResult<Void>> collecting = query.collecting(this);
            return collecting.execute(Tuple.tuple(values));
        }).onSuccess(new Handler<SqlResult<Void>>() {
            @Override
            public void handle(SqlResult<Void> event) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("subscribeActual successful sql:{} connection:{}", sql, sqlConnectionFuture.result());
                }
                observer.onComplete();
            }
        }).onFailure(event -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("subscribeActual error sql:{}", sql);
            }
            this.observer.onError(event);
        });
    }

    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
        this.columnDefinitions = columnDefinitions.columnDefinitions();
        if (this.metaData == null) {
            this.metaData = toColumnMetaData(columnDefinitions.columnDescriptor());
        }
        this.observer.onSubscribe(Disposable.disposed());
    }

    @Override
    public void onRow(Row row) {
        MycatRowMetaData metaData = this.metaData;
        int columnCount = this.metaData.getColumnCount();
        Object[] objects = getObjects(row, metaData);
        observer.onNext(objects);
    }

    @NotNull
    public static Object[] getObjects(Row row, MycatRowMetaData metaData) {
        Object[] objects = new Object[metaData.getColumnCount()];
        for (int columnIndex = 0; columnIndex < objects.length; columnIndex++) {
            int columnType = metaData.getColumnType(columnIndex);
            Object value = null;
            switch (columnType) {
                case BOOLEAN:
                case BIT: {
                    Numeric numeric = row.getNumeric(columnIndex);
                    if (numeric == null) {
                        value = null;
                    } else {
                        value = MycatValueFactory.BOOLEAN_VALUE_FACTORY.createFromLong(numeric.longValue());
                    }
                    break;
                }
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                {
                    Numeric numeric = row.getNumeric(columnIndex);
                    if (numeric == null) {
                        value = null;
                    } else {
                        value = MycatValueFactory.LONG_VALUE_FACTORY.createFromLong(numeric.longValue());
                    }
                    break;
                }

                case FLOAT:
                case REAL:
                case DOUBLE: {
                    Numeric numeric = row.getNumeric(columnIndex);
                    if (numeric == null) {
                        value = null;
                    } else {
                        value = MycatValueFactory.DOUBLE_VALUE_FACTORY.createFromDouble(numeric.longValue());
                    }
                    break;
                }

                case DECIMAL:
                case NUMERIC: {
                    value = row.getBigDecimal(columnIndex);
                    break;
                }
                case NCHAR:
                case NVARCHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case VARCHAR:
                case CHAR: {
                    value = row.getString(columnIndex);
                    break;
                }
                case DATE: {
                    value = row.getLocalDate(columnIndex);
                    break;
                }
                case TIME_WITH_TIMEZONE:
                case TIME: {
                    value = row.getValue(columnIndex);
                    break;
                }
                case TIMESTAMP_WITH_TIMEZONE:
                case TIMESTAMP: {
                    value = row.getValue(columnIndex);
                    break;
                }
                case CLOB:
                case NCLOB:
                case BLOB:
                case LONGVARBINARY:
                case VARBINARY:
                case BINARY: {
                    value = row.getValue(columnIndex);
                    if (value == null) {
                        value = null;
                    } else if (value instanceof String) {
                        value = ((String) value).getBytes();
                    } else if (value instanceof Buffer) {
                        value = ((Buffer) value).getBytes();
                    }
                    break;
                }
                case NULL: {
                    value = null;
                    break;
                }
                case ROWID:
                case REF_CURSOR:
                case OTHER:
                case JAVA_OBJECT:
                case DISTINCT:
                case STRUCT:
                case ARRAY:
                case REF:
                case DATALINK:
                default:
                    value = row.getValue(columnIndex);
                    LOGGER.warn("may be unsupported type :" + JDBCType.valueOf(columnType));
            }
            objects[columnIndex] = value;
        }
        return objects;
    }


    public static MycatRowMetaData toColumnMetaData(List<ColumnDescriptor> event) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        for (ColumnDescriptor columnDescriptor : event) {
            resultSetBuilder.addColumnInfo(columnDescriptor.name(),
                    columnDescriptor.jdbcType());
        }
        RowBaseIterator build = resultSetBuilder.build();
        return build.getMetaData();
    }
}

package io.mycat.vertx;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.Row;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EmitterObjectsCollector implements StreamMysqlCollector {
    protected final ObservableEmitter<Object[]> emitter;
    protected MycatRowMetaData rowMetaData;
    protected int currentRowCount;

    public EmitterObjectsCollector(ObservableEmitter<Object[]> emitter,
                                   MycatRowMetaData rowMetaData) {
        this.emitter = emitter;
        this.rowMetaData = rowMetaData;
    }

    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
        if (this.rowMetaData == null) {
            this.rowMetaData = BaseRowObservable.toColumnMetaData(
                    columnDefinitions.columnDescriptor());
        }
    }

    @Override
    public void onRow(Row row) {
        currentRowCount++;
        emitter.onNext(BaseRowObservable.getObjects(row, rowMetaData));
    }

}

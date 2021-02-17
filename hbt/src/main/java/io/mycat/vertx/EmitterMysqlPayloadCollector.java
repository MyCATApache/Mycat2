package io.mycat.vertx;

import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.Row;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EmitterMysqlPayloadCollector implements StreamMysqlCollector {
    protected final ObservableEmitter<MysqlPayloadObject> emitter;
    protected MycatRowMetaData rowMetaData;
    protected boolean outputColumnDef;
    protected int currentRowCount;
    public EmitterMysqlPayloadCollector(ObservableEmitter<MysqlPayloadObject> emitter,
                                        MycatRowMetaData rowMetaData, boolean outputMetaDataPacket) {
        this.emitter = emitter;
        this.rowMetaData = rowMetaData;
        this.outputColumnDef = outputMetaDataPacket;
    }
    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
        if (this.rowMetaData == null) {
            this.rowMetaData = BaseRowObservable.toColumnMetaData(
                    columnDefinitions.columnDescriptor());
        }
        if (this.outputColumnDef) {
            MysqlPayloadObject packet = new MySQLColumnDef(this.rowMetaData);
            emitter.onNext(packet);
        }
    }

    @Override
    public void onRow(Row row) {
        currentRowCount++;
        MysqlPayloadObject packet = new MysqlRow(BaseRowObservable.getObjects(row, rowMetaData));
        emitter.onNext(packet);
    }

}

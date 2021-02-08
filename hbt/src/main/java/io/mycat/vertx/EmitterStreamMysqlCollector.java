package io.mycat.vertx;

import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.*;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EmitterStreamMysqlCollector implements StreamMysqlCollector {
    private final ObservableEmitter<MysqlPacket> emitter;

    public EmitterStreamMysqlCollector(ObservableEmitter<MysqlPacket> emitter) {
        this.emitter = emitter;
    }

    private MySQLColumnDef columnDef;
    private int currentRowCount;

    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
        MysqlPacket packet = new MySQLColumnDef(columnDefinitions, queryCommand);
        emitter.onNext(packet);
    }

    @Override
    public void onRow(Row row) {
        currentRowCount++;
        MysqlPacket packet = new MysqlRow(columnDef,row);
        emitter.onNext(packet);
    }

    @Override
    public void onFinish(int sequenceId, int serverStatusFlags, long affectedRows, long lastInsertId) {
        MysqlPacket packet = new MysqlEnd(sequenceId, serverStatusFlags, affectedRows, lastInsertId);
        emitter.onNext(packet);
    }
}

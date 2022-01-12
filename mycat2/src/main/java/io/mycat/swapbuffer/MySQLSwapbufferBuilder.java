package io.mycat.swapbuffer;

import io.mycat.MySQLPacketUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.vertx.ResultSetMapping;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.vertx.core.buffer.Buffer;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class MySQLSwapbufferBuilder {
    int packetId = 1;
    List<RowBaseIterator> rowBaseIteratorList;
    boolean binary;

    public MySQLSwapbufferBuilder(List<RowBaseIterator> rowBaseIteratorList) {
        this.rowBaseIteratorList = rowBaseIteratorList;
    }
    public MySQLSwapbufferBuilder(RowBaseIterator rowBaseIterator) {
       this(Collections.singletonList(rowBaseIterator));
    }

    public Observable<Buffer> build() {
        return Observable.create(new ObservableOnSubscribe<Buffer>() {
            @Override
            public void subscribe(@NonNull ObservableEmitter<Buffer> emitter) throws Throwable {
                try {
                    int resultSetCount = rowBaseIteratorList.size();
                    Function<Object[], byte[]> function;
                    for (RowBaseIterator baseIterator : rowBaseIteratorList) {
                        resultSetCount--;
                        MycatRowMetaData metaData = baseIterator.getMetaData();

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

                        emitter.onNext(packet(
                                MySQLPacketUtil.generateMySQLPacket(packetId++,
                                        MySQLPacketUtil.generateEof(0, 0))
                        ));


                        function =
                                !binary ?
                                        ResultSetMapping.concertToDirectTextResultSet(metaData) :
                                        ResultSetMapping.concertToDirectBinaryResultSet(metaData);

                        while (baseIterator.next()) {
                            emitter.onNext(packet(
                                    MySQLPacketUtil.generateMySQLPacket(packetId++,
                                            function.apply(baseIterator.getObjects()))
                            ));
                        }

                        emitter.onNext(packet(
                                MySQLPacketUtil.generateMySQLPacket(packetId++,
                                        MySQLPacketUtil.generateEof(0, resultSetCount != 0 ? MySQLServerStatusFlags.MORE_RESULTS : 0))
                        ));
                    }
                    emitter.onComplete();
                } catch (Exception e) {
                    emitter.tryOnError(e);
                }
            }
            private Buffer packet(byte[] generateMySQLPacket) {
                return Buffer.buffer(generateMySQLPacket);
            }
        });
    }
}

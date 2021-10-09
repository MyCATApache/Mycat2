package io.mycat.api.collector;

import lombok.Getter;

@Getter
public class MysqlByteArrayPayloadRow implements MysqlPayloadObject {
   final byte[] bytes;

    public MysqlByteArrayPayloadRow(byte[] bytes) {
        this.bytes = bytes;
    }

    public static MysqlByteArrayPayloadRow of(byte[] bytes){
        return new MysqlByteArrayPayloadRow(bytes);
    }
}

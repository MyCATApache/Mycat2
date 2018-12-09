package io.mycat.mysql;
/*
cjw
294712221@qq.com
 */
public enum PacketType {
    FULL(PayloadType.UNKNOWN, PayloadType.FULL_PAYLOAD),
    LONG_HALF(PayloadType.UNKNOWN, PayloadType.LONG_PAYLOAD),
    SHORT_HALF(PayloadType.SHORT_PAYLOAD, PayloadType.SHORT_PAYLOAD),
    REST_CROSS(PayloadType.REST_CROSS_PAYLOAD, PayloadType.UNKNOWN),
    FINISHED_CROSS(PayloadType.FINISHED_CROSS_PAYLOAD, PayloadType.UNKNOWN),
    BINDATA(PayloadType.UNKNOWN, PayloadType.UNKNOWN);
    PayloadType corssPayloadType;
    PayloadType fullPayloadType;

    PacketType(PayloadType corssPayloadType, PayloadType fullPayloadType) {
        this.corssPayloadType = corssPayloadType;
        this.fullPayloadType = fullPayloadType;
    }
}
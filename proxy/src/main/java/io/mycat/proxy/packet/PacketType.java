package io.mycat.proxy.packet;
/*
cjw
294712221@qq.com
 */
public enum PacketType {
    FULL(),
    LONG_HALF(),
    SHORT_HALF(),
    REST_CROSS(),
    FINISHED_CROSS(),
}
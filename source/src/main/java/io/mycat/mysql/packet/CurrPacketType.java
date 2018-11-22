package io.mycat.mysql.packet;

/**
 * 当前接收到的包类型
 * cjw
 * 294712221@qq.com
 */
public enum CurrPacketType {
        Full, LongHalfPacket, ShortHalfPacket, RestCrossBufferPacket, FinishedCrossBufferPacket
    }
package io.mycat.mysql.packet;

// 当前接收到的包类型
public enum CurrPacketType {
        Full, LongHalfPacket, ShortHalfPacket, RestCrossBufferPacket, FinishedCrossBufferPacket
    }
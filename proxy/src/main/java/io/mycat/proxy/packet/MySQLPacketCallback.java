package io.mycat.proxy.packet;

public interface MySQLPacketCallback {

    default void onRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareLongData(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onError(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onColumnCount(int columnCount) {

    }

    default void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onColumnDefEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onRowEof(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    void onFinished(boolean success, String errorMessage);

    default void onRowOk(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onRowError(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOk(PreparedOKPacket preparedOKPacket) {

    }

    default void onPrepareOkParameterDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOkColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    default void onPrepareOkColumnDefEof(EOFPacket packet) {

    }

    default void onPrepareOkParameterDefEof(EOFPacket packet) {

    }

    default void onLoadDataRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }
}

package io.mycat.proxy.packet;

public interface ResultSetCollector {
    void onResultSetStart();

    void onResultSetEnd();

    void collectColumnList(ColumnDefPacket[] packets);

    void onRowStart();

    void onRowEnd();

    void collectDecimal(int columnIndex,ColumnDefPacket columnDef,int decimalScale, MySQLPacket mySQLPacket, int startIndex);

    void collectTiny(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectGeometry(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTinyString(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectVarString(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectShort(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectMediumBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTinyBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectFloat(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDouble(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectNull(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTimestamp(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectInt24(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDate(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTime(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDatetime(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectYear(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectNewDate(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectVarChar(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectBit(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectNewDecimal(int columnIndex,ColumnDefPacket columnDef,int decimalScale, MySQLPacket mySQLPacket, int startIndex);

    void collectEnum(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectSet(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLong(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLongLong(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLongBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);
}

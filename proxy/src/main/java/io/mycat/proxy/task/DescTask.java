package io.mycat.proxy.task;

import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.packet.ColumnDefPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.ResultSetCollector;

import static io.mycat.beans.mysql.MySQLFieldsType.*;

public class DescTask implements ResultSetTask {
    int columnCount;
    int[] resultSetColumnTypeList;
    ColumnDefPacket[] currentColumnDefList;
    ResultSetCollector collector = new ResultSetCollector() {
        @Override
        public void onResultSetStart() {

        }

        @Override
        public void onResultSetEnd() {

        }

        @Override
        public void collectColumnList(ColumnDefPacket[] packets) {

        }

        @Override
        public void onRowStart() {

        }

        @Override
        public void onRowEnd() {

        }

        @Override
        public void collectDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectTiny(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectGeometry(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectTinyString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectVarString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectShort(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectMediumBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectTinyBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectFloat(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectDouble(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectNull(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectTimestamp(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectInt24(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectTime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectDatetime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectYear(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectNewDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectVarChar(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectBit(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectEnum(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectSet(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectLongLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }

        @Override
        public void collectLongBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {

        }
    };

    @Override
    public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
        ColumnDefPacket packet = new ColumnDefPacketImpl();
        ((ColumnDefPacketImpl) packet).read(mySQLPacket, startPos, endPos);
        int i = this.columnCount++;
        this.resultSetColumnTypeList[i] = packet.getColumnType();
        this.currentColumnDefList[i] = packet;
    }

    @Override
    public void onColumnCount(int columnCount) {
        this.columnCount = 0;
        this.resultSetColumnTypeList = new int[columnCount];
        this.currentColumnDefList = new ColumnDefPacket[columnCount];
        collector.onResultSetStart();
    }

    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {
        for (int columnIndex = 0; columnIndex < this.columnCount; columnIndex++) {

            ColumnDefPacket columnDef = currentColumnDefList[columnIndex];
            int startIndex = mySQLPacket.packetReadStartIndex();
            switch (resultSetColumnTypeList[columnIndex]) {
                default: {
                    throw new MycatExpection("");
                }
                case FIELD_TYPE_DECIMAL: {
                    collector.collectDecimal(columnIndex, columnDef, columnDef.getColumnDecimals() & 0xff, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_TINY: {
                    collector.collectTiny(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_SHORT: {
                    collector.collectShort(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_LONG: {
                    collector.collectLong(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_FLOAT: {
                    collector.collectFloat(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_DOUBLE: {
                    collector.collectDouble(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_NULL: {
                    collector.collectNull(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_TIMESTAMP: {
                    collector.collectTimestamp(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_LONGLONG: {
                    collector.collectLongLong(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_INT24: {
                    collector.collectInt24(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_DATE: {
                    collector.collectDate(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_TIME: {
                    collector.collectTime(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_DATETIME: {
                    collector.collectDatetime(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_YEAR: {
                    collector.collectYear(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_NEWDATE: {
                    collector.collectNewDate(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_VARCHAR: {
                    collector.collectVarChar(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_BIT: {
                    collector.collectBit(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_NEW_DECIMAL: {
                    collector.collectNewDecimal(columnIndex, columnDef, columnDef.getColumnDecimals() & 0xff, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_ENUM: {
                    collector.collectEnum(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_SET: {
                    collector.collectSet(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_TINY_BLOB: {
                    collector.collectTinyBlob(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_MEDIUM_BLOB: {
                    collector.collectMediumBlob(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_LONG_BLOB: {
                    collector.collectLongBlob(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_BLOB: {
                    collector.collectBlob(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_VAR_STRING: {
                    collector.collectVarString(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_STRING: {
                    collector.collectTinyString(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
                case FIELD_TYPE_GEOMETRY: {
                    collector.collectGeometry(columnIndex, columnDef, mySQLPacket, startIndex);
                    break;
                }
            }
        }


}

    @Override
    public Object getResult() {
        return null;
    }
}

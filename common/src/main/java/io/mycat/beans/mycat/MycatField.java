package io.mycat.beans.mycat;

import com.mysql.cj.CharsetMapping;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class MycatField {
    private final String name;
    private final MycatDataType mycatDataType;
    private final byte scale;
    private final byte precision;
    private final boolean nullable;

    public MycatField(String name, MycatDataType mycatDataType, boolean nullable) {
        this.name = name;
        this.mycatDataType = mycatDataType;
        this.scale = 0;
        this.precision = 0;
        this.nullable = nullable;
    }


    public MycatField(String name, MycatDataType mycatDataType, boolean nullable, int scale, int precision) {
        this.name = name;
        this.mycatDataType = mycatDataType;
        this.scale = (byte) scale;
        this.precision = (byte) precision;
        this.nullable = nullable;
    }

    public static MycatField of(String name, MycatDataType mycatDataType, boolean nullable) {
        return new MycatField(name, mycatDataType, nullable, 0, 0);
    }

    public static MycatField of(String name, MycatDataType mycatDataType, boolean nullable, int scale, int precision) {
        return new MycatField(name, mycatDataType, nullable, scale, precision);
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

   public ColumnDefPacketImpl toColumnDefPacket() {

        int mysqlColumnType = 0;
        int columnFlags = 0;
        int columnLength = 255;
        int columnCharsetSet = 0x21;

        if (!nullable) {
            columnFlags |= MySQLFieldsType.NOT_NULL_FLAG;
        }

        switch (mycatDataType) {
            case BOOLEAN:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_BIT;
                columnLength = 1;
                break;
            case BIT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_BIT;
                break;
            case TINYINT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_TINY;
                break;
            case UNSIGNED_TINYINT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_TINY;
                columnFlags = columnFlags | MySQLFieldsType.UNSIGNED_FLAG;
                break;
            case SHORT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_SHORT;
                break;
            case UNSIGNED_SHORT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_SHORT;
                columnFlags = columnFlags | MySQLFieldsType.UNSIGNED_FLAG;
                break;
            case INT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_LONG;
                break;
            case UNSIGNED_INT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_LONG;
                columnFlags = columnFlags | MySQLFieldsType.UNSIGNED_FLAG;
                break;
            case LONG:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_LONGLONG;
                break;
            case UNSIGNED_LONG:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_LONGLONG;
                columnFlags = columnFlags | MySQLFieldsType.UNSIGNED_FLAG;
                break;
            case DOUBLE:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_DOUBLE;
                break;
            case DECIMAL:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_DECIMAL;
                break;
            case DATE:
                columnFlags = MySQLFieldsType.FIELD_TYPE_DATE;
                break;
            case DATETIME:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_DATETIME;
                columnFlags |= MySQLFieldsType.TIMESTAMP_FLAG;
                break;
            case TIME:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_TIME;
                break;
            case YEAR:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_YEAR;
                break;
            case CHAR:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_STRING;
                break;
            case VARCHAR:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_VARCHAR;
                break;
            case BINARY:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_BLOB;
                columnCharsetSet = CharsetMapping.MYSQL_COLLATION_INDEX_binary;
                columnFlags |= MySQLFieldsType.BLOB_FLAG;
                columnFlags |= MySQLFieldsType.BINARY_FLAG;
                break;
            case FLOAT:
                mysqlColumnType = MySQLFieldsType.FIELD_TYPE_FLOAT;
                break;
        }
        ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
        byte[] nameBytes = name.getBytes();
        columnDefPacket.setColumnName(nameBytes);
        columnDefPacket.setColumnOrgName(nameBytes);
        columnDefPacket.setColumnType(mysqlColumnType);
        columnDefPacket.setColumnFlags(columnFlags);
        columnDefPacket.setColumnLength(columnLength);
        columnDefPacket.setColumnDecimals(scale);
        columnDefPacket.setColumnCharsetSet(columnCharsetSet);
        return columnDefPacket;
    }
    public MycatField rename(String newName){
        return of(newName,mycatDataType,nullable,scale,precision);
    }
}

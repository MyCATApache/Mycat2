package io.mycat.beans;

import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.packet.ColumnDefPacketImpl;

public class MySQLFieldInfo {
    String schemaName;
    String tableName;
    String name;
    int fieldType;
    int ordinalPosition;

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    int length;
    int fieldDetailFlag;
    String comment;
    String charset;
    int collationId;
    byte decimals;
    byte[] defaultValues;

    public ColumnDefPacket toColumnDefPacket(String alias) {
        ColumnDefPacket columnDefPacket = new ColumnDefPacketImpl();
        columnDefPacket.setColumnCatalog(columnDefPacket.DEFAULT_CATALOG);
        columnDefPacket.setColumnSchema(getSchemaName().getBytes());
        columnDefPacket.setColumnTable(getTableName().getBytes());
        if (alias == null) {
            alias = getName();
        }
        columnDefPacket.setColumnName(alias.getBytes());
        columnDefPacket.setColumnOrgName(getName().getBytes());
        columnDefPacket.setColumnNextLength(0xC);
        columnDefPacket.setColumnCharsetSet(getCollationId());
        columnDefPacket.setColumnLength(getColumnMaxLength());
        columnDefPacket.setColumnType(getFieldType());
        columnDefPacket.setColumnFlags(getFieldDetailFlag());
        columnDefPacket.setColumnDecimals(getDecimals());
        columnDefPacket.setColumnDefaultValues(getDefaultValues());
        return columnDefPacket;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] getDefaultValues() {
        return defaultValues;
    }

    public void setDefaultValues(byte[] defaultValues) {
        this.defaultValues = defaultValues;
    }

    public byte getDecimals() {
        return decimals;
    }

    public void setDecimals(byte decimals) {
        this.decimals = decimals;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFieldType(int fieldType) {
        this.fieldType = fieldType;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setCollationId(int collationId) {
        this.collationId = collationId;
    }

    public int getColumnMaxLength() {
        return columnMaxLength;
    }

    public void setColumnMaxLength(int columnMaxLength) {
        this.columnMaxLength = columnMaxLength;
    }

    int columnMaxLength;

    public String getName() {
        return name;
    }

    public int getFieldType() {
        return fieldType;
    }

    public int getLength() {
        return length;
    }

    public String getComment() {
        return comment;
    }

    public String getCharset() {
        return charset;
    }

    public int getCollationId() {
        return collationId;
    }


    public int getFieldDetailFlag() {
        return fieldDetailFlag;
    }

    public void setFieldDetailFlag(int fieldDetailFlag) {
        this.fieldDetailFlag = fieldDetailFlag;
    }

    public void setNotNullable() {
        this.fieldDetailFlag |= MySQLFieldsType.NOT_NULL_FLAG;
    }

    public void setNullable() {
        this.fieldDetailFlag &= ~MySQLFieldsType.NOT_NULL_FLAG;
    }
}

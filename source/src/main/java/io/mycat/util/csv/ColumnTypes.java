package io.mycat.util.csv;

import java.util.HashMap;
import java.util.Map;

/**
 * ${todo}
 *
 * @author : zhuqiang
 * @date : 2018/12/2 14:50
 */
public enum ColumnTypes {
    /**
     * binary COLLATE binary (63)
     */
    MYSQL_TYPE_DECIMAL(0x00, 10, 0, 0, 63, 0),
    MYSQL_TYPE_TINY(0x01, 4, 0, 0, 63, 0),
    MYSQL_TYPE_SHORT(0x02, 6, 0, 0, 63, 0),
    MYSQL_TYPE_LONG(0x03, 11, 0, 0, 63, 0),
    MYSQL_TYPE_FLOAT(0x04, 0, 31, 0, 63, 0),
    MYSQL_TYPE_DOUBLE(0x05, 0, 31, 0, 63, 0),
    MYSQL_TYPE_NULL(0x06, 0, 0, 0, 0, 0), // 未确认其他信息
    MYSQL_TYPE_TIMESTAMP(0x07, 0, 0, 0, 63, 0x0080),
    MYSQL_TYPE_LONGLONG(0x08, 0, 0, 0, 63, 0),
    MYSQL_TYPE_INT24(0x09, 0, 0, 0, 63, 0),
    MYSQL_TYPE_DATE(0x0a, 0, 0, 0, 63, 0x0080),
    MYSQL_TYPE_TIME(0x0b, 0, 0, 0, 63, 0x0080),
    MYSQL_TYPE_DATETIME(0x0c, 0, 0, 0, 63, 0x0080),
    MYSQL_TYPE_YEAR(0x0d, 4, 0, 0, 63, 0x0060),
    MYSQL_TYPE_NEWDATE(0x0e, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_VARCHAR(0x0f, 255, 0, 0, 0, 0),
    MYSQL_TYPE_BIT(0x10, 1, 0, 0, 63, 0x0020),
    MYSQL_TYPE_TIMESTAMP2(0x11, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_DATETIME2(0x12, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_TIME2(0x13, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_NEWDECIMAL(0xf6, 10, 0, 0, 63, 0),
    MYSQL_TYPE_ENUM(0xf7, 0, 0, 0, 0, 0x0100),
    MYSQL_TYPE_SET(0xf8, 0, 0, 0, 0, 0x0800),
    MYSQL_TYPE_TINY_BLOB(0xf9, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_MEDIUM_BLOB(0xfa, 0, 0, 0, 0, 0),// 未确认其他信息
    MYSQL_TYPE_LONG_BLOB(0xfb, 0, 0, 0, 0, 0),// 未确认其他信息
    BLOB_MYSQL_TYPE_BLOB(0xfc, 65535, 0, 0, 63, 0x0090),
    TEXT_MYSQL_TYPE_BLOB(0xfc, 196605, 0, 0, 0, 0x0010),
    VARCHAR_MYSQL_TYPE_VAR_STRING(0xfd, 255, 0, 0, 0, 0),
    VARBINARY_MYSQL_TYPE_VAR_STRING(0xfd, 255, 0, 0, 63, 0x0080),
    CHAR_MYSQL_TYPE_STRING(0xfe, 255, 0, 0, 0, 0),
    BINARY_MYSQL_TYPE_STRING(0xfe, 0, 0, 0, 63, 0x0080),
    ENUM_MYSQL_TYPE_STRING(0xfe, 0, 0, 0, 0, 0x0100),
    SET_MYSQL_TYPE_STRING(0xfe, 0, 0, 0, 0, 0x0800),
    MYSQL_TYPE_GEOMETRY(0xff, 0, 0, 0, 0, 0),// 未确认其他信息
    ;
    private int value;
    //----- 以下类型标识默认值
    /**
     * 对于浮点和定点类型， M是可以存储的总位数（精度）。对于字符串类型， M是最大长度。允许的最大值取决于数据类型
     */
    private int m;
    /**
     * D适用于浮点和定点类型，并指示小数点后面的位数（刻度）。最大可能值为30，但不应大于 M-2
     */
    private int d;
    /**
     * 适用于 TIME， DATETIME和 TIMESTAMP类型和表示小数精度秒; 也就是说，小数部分秒的小数点后面的位数。的 fsp值，如果给定的，必须在1到6的值为0表示没有小数部分范围为0。如果省略，则默认精度为0
     */
    private int fsp;
    /**
     * 对于非字符串类型有默认编码，但是都是一样的是 63
     */
    private int ccharetIndex;
    /**
     * 状态，能力标识
     */
    private int status;
    /**
     * 数据类型和协议类型的关联关系
     */
    private static Map<String, ColumnTypes> MAPS = new HashMap<>();

    static {
        MAPS.put("INT", MYSQL_TYPE_LONG);
        MAPS.put("SMALLINT", MYSQL_TYPE_SHORT);
        MAPS.put("TINYINT", MYSQL_TYPE_TINY);
        MAPS.put("MEDIUMINT", MYSQL_TYPE_INT24);
        MAPS.put("BIGINT", MYSQL_TYPE_LONGLONG);
        MAPS.put("DECIMAL", MYSQL_TYPE_NEWDECIMAL);
        MAPS.put("FLOAT", MYSQL_TYPE_FLOAT);
        MAPS.put("DOUBLE", MYSQL_TYPE_DOUBLE);
        MAPS.put("BIT", MYSQL_TYPE_BIT);
        MAPS.put("DATE", MYSQL_TYPE_DATE);
        MAPS.put("DATETIME", MYSQL_TYPE_DATETIME);
        MAPS.put("TIMESTAMP", MYSQL_TYPE_TIMESTAMP);
        MAPS.put("TIME", MYSQL_TYPE_TIME);
        MAPS.put("YEAR", MYSQL_TYPE_YEAR);
        MAPS.put("CHAR", CHAR_MYSQL_TYPE_STRING);
        MAPS.put("VARCHAR", VARCHAR_MYSQL_TYPE_VAR_STRING);
        MAPS.put("BINARY", BINARY_MYSQL_TYPE_STRING);
        MAPS.put("VARBINARY", VARBINARY_MYSQL_TYPE_VAR_STRING);
        MAPS.put("BLOB", BLOB_MYSQL_TYPE_BLOB);
        MAPS.put("TEXT", TEXT_MYSQL_TYPE_BLOB);
        MAPS.put("ENUM", ENUM_MYSQL_TYPE_STRING);
        MAPS.put("SET", SET_MYSQL_TYPE_STRING);
    }

    ColumnTypes(int value, int m, int d, int fsp, int ccharetIndex, int status) {
        this.value = value;
        this.m = m;
        this.d = d;
        this.fsp = fsp;
        this.ccharetIndex = ccharetIndex;
        this.status = status;
    }

    /**
     * 根据数据类型获取协议类型
     */
    public static ColumnTypes find(String dataType) {
        return MAPS.get(dataType.toUpperCase());
    }

    public int getValue() {
        return value;
    }

    public int getM() {
        return m;
    }

    public int getD() {
        return d;
    }

    public int getFsp() {
        return fsp;
    }

    public int getCcharetIndex() {
        return ccharetIndex;
    }

    public int getStatus() {
        return status;
    }

    public static void main(String[] args) {
        int a = 0x0080;
        System.out.println(a);
    }
}

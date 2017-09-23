package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl;

public enum SQLType {
    INSERT(14), DELETE(13), REPLACE(15), SELECT(11), UPDATE(12);
int value;

    SQLType(int value) {
        this.value = value;
    }
    public static SQLType getSQLTypeByValue(int value) {
        for (SQLType c : SQLType.values()) {
            if (c.value == value) {
                return c;
            }
        }
        return null;
    }
   public int getValue(){
        return value;
   }
}
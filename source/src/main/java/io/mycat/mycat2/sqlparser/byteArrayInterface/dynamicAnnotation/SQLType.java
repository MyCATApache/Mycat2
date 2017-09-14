package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

public enum SQLType {
    INSERT("INSERT"), DELETE("DELETE"), REPLACE("REPLACE"), SELECT("SELECT"), UPDATE("UPDATE");
    String text;

    SQLType(String v) {
        text = v;
    }
}
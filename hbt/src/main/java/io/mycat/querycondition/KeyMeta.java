package io.mycat.querycondition;

import lombok.Getter;

@Getter
public class KeyMeta {
    String indexName;
    String columnName;

    public KeyMeta(String indexName, String columnName) {
        this.indexName = indexName;
        this.columnName = columnName;
    }

    public boolean findColumnName(String name) {
        return columnName.equals(name);
    }

    public static KeyMeta of(String indexName, String columnName) {
        return new KeyMeta(indexName, columnName);
    }


}

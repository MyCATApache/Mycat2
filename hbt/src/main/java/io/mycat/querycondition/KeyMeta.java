package io.mycat.querycondition;

import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Getter
public class KeyMeta {
    String indexName;
    List<String> columnNames;

    public KeyMeta(String indexName,  List<String>  columnNames) {
        this.indexName = indexName;
        this.columnNames = columnNames;
    }

    public boolean findColumnName(String name) {
        return columnNames.contains(name);
    }
    public static KeyMeta of(String indexName,  String  columnName) {
        return new KeyMeta(indexName, Collections.singletonList( columnName));
    }
    public static KeyMeta of(String indexName,  List<String>  columnNames) {
        return new KeyMeta(indexName, columnNames);
    }


}

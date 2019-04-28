package io.mycat.beans.mysql;

import java.util.HashMap;
import java.util.Map;

public class MySQLCollationTable {
    final Map<Integer, MySQLCollation> indexMap = new HashMap<>();
    final Map<String, MySQLCollation> collationNameMap = new HashMap<>();

    public MySQLCollation getCollationById(Integer id) {
        return indexMap.get(id);
    }

//    public MySQLCollation getByCharsetName(String charsetName) {
//        return charsetNameMap.get(charsetName);
//    }

    public MySQLCollation getByCollationName(String collationName) {
        return collationNameMap.get(collationName);
    }

    public void put(MySQLCollation collation) {
        collationNameMap.put(collation.getCollatioNname(), collation);
        indexMap.put(collation.getId(), collation);
    }
}

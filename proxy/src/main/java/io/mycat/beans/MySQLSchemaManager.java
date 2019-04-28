package io.mycat.beans;

import io.mycat.beans.mysql.MySQLCollation;
import io.mycat.beans.mysql.MySQLCollationTable;
import io.mycat.beans.mysql.MySQLFieldsType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MySQLSchemaManager {
    Map<String, Map<String, MySQLTableInfo>> dataBase = new HashMap<>();
    MySQLCollationTable collationTable = new MySQLCollationTable();
    public MySQLSchemaManager() {
        MySQLCollation collation = new MySQLCollation();
        collation.setCharsetName("utf8");
        collation.setCollatioNname("utf8_general_ci");
        collation.setId(33);
        collation.setDefault(true);
        collation.setCompiled(true);
        collation.setSortLen(1);
        collationTable.put(collation);

        String schemaName = "mycat_schema";
        String tableName ="mycat_test";
        MySQLTableInfo tableInfo = new MySQLTableInfo();
        tableInfo.setSchemaName(schemaName);

        MySQLFieldInfo fieldInfo = new MySQLFieldInfo();
        fieldInfo.setCharset("utf8");
        fieldInfo.setSchemaName(schemaName);
        fieldInfo.setTableName(tableName);
        fieldInfo.setCollationId(33);
        fieldInfo.setOrdinalPosition(1);
        fieldInfo.setDefaultValues(new byte[]{});
        fieldInfo.setNullable();
        fieldInfo.setFieldType(MySQLFieldsType.FIELD_TYPE_LONG);
        fieldInfo.setName("id");

        tableInfo.putField(fieldInfo);

         fieldInfo = new MySQLFieldInfo();
        fieldInfo.setCharset("utf8");
        fieldInfo.setSchemaName("mycat_schema");
        fieldInfo.setTableName("mycat_test");
        fieldInfo.setCollationId(33);
        fieldInfo.setOrdinalPosition(1);
        fieldInfo.setDefaultValues(new byte[]{});
        fieldInfo.setNullable();
        fieldInfo.setFieldType(MySQLFieldsType.FIELD_TYPE_STRING);
        fieldInfo.setName("name");

        tableInfo.putField(fieldInfo);

        dataBase.compute(schemaName, new BiFunction<String, Map<String, MySQLTableInfo>, Map<String, MySQLTableInfo>>() {
            @Override
            public Map<String, MySQLTableInfo> apply(String s, Map<String, MySQLTableInfo> map) {
                if(map == null){
                    map = new HashMap<>();
                }
                map.put(tableName,  tableInfo);
                return map;
            }
        });
    }


    MySQLTableInfo getTableInfoBySchemaNameTableName(String schamaName, String tableName) {
        return dataBase.get(schamaName).get(tableName);
    }
}

package io.mycat;

import io.mycat.beans.mysql.InformationSchema;
import io.mycat.beans.mysql.InformationSchemaRuntime;
import io.mycat.metadata.MetadataManager;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public enum  DDLManager {
    INSTANCE;
    public void updateTables() {
        List<TableHandler> collect = MetadataManager.INSTANCE.getSchemaMap().values().stream().distinct()
                .flatMap(i -> i.logicTables().values().stream()).distinct().collect(Collectors.toList());
        ArrayList<InformationSchema.TABLES_TABLE_OBJECT> objects = new ArrayList<>();
        for (TableHandler value : collect) {
            String TABLE_CATALOG = "def";
            String TABLE_SCHEMA = value.getSchemaName();
            String TABLE_NAME = value.getTableName();
            String TABLE_TYPE = "BASE TABLE";
            String ENGINE = "InnoDB";
            Long VERSION = 10L;
            String ROW_FORMAT = "DYNAMIC";
            InformationSchema.TABLES_TABLE_OBJECT tableObject = InformationSchema.TABLES_TABLE_OBJECT.builder()
                    .TABLE_CATALOG(TABLE_CATALOG)
                    .TABLE_SCHEMA(TABLE_SCHEMA)
                    .TABLE_NAME(TABLE_NAME)
                    .TABLE_TYPE(TABLE_TYPE)
                    .ENGINE(ENGINE)
                    .VERSION(VERSION)
                    .ROW_FORMAT(ROW_FORMAT)
                    .build();
            objects.add(tableObject);
        }
        InformationSchema.TABLES_TABLE_OBJECT[] tables_table_objects = objects.toArray(new InformationSchema.TABLES_TABLE_OBJECT[0]);
        InformationSchemaRuntime.INSTANCE.update(informationSchema -> informationSchema.TABLES = tables_table_objects);
    }
}
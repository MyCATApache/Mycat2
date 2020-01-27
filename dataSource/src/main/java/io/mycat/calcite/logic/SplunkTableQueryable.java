package io.mycat.calcite.logic;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

public  class SplunkTableQueryable<T>
            extends AbstractTableQueryable<T> {
        SplunkTableQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName, QueryableTable table) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            return null;
        }


    }
package io.mycat.beans;

import io.mycat.config.schema.TableDefConfig;

public class TableDef {
    final TableDefConfig tableDefConfig;

    public TableDef(TableDefConfig tableDefConfig) {
        this.tableDefConfig = tableDefConfig;
    }
}

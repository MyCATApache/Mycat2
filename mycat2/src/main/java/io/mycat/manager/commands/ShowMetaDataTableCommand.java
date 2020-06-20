package io.mycat.manager.commands;

import io.mycat.LogicTableType;
import io.mycat.MycatDataContext;
import io.mycat.SimpleColumnInfo;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.ShardingTable;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ShowMetaDataTableCommand implements ManageCommand{
    @Override
    public String statement() {
        return "show @@metadata.schema.table";
    }

    @Override
    public String description() {
        return "show @@metadata.schema.table";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("SCHEMA_NAME", JDBCType.VARCHAR)
                .addColumnInfo("TABLE_NAME",JDBCType.VARCHAR)
                .addColumnInfo("CREATE_TABLE_SQL",JDBCType.VARCHAR)
                .addColumnInfo("TYPE",JDBCType.VARCHAR)
                .addColumnInfo("COLUMNS",JDBCType.VARCHAR);
        MetadataManager.INSTANCE.getSchemaMap().values().stream().flatMap(i->i.logicTables().values().stream()).forEach(table->{
            String SCHEMA_NAME = table.getSchemaName();
            String TABLE_NAME = table.getTableName();
            String CREATE_TABLE_SQL = table.getCreateTableSQL();
            LogicTableType TYPE = table.getType();
            String COLUMNS = table.getColumns().stream().map(i -> i.toString()).collect(Collectors.joining(","));
            builder.addObjectRowPayload(Arrays.asList(SCHEMA_NAME,TABLE_NAME,CREATE_TABLE_SQL,TYPE,COLUMNS));
        });
        response.sendResultSet(()->builder.build());
    }
}
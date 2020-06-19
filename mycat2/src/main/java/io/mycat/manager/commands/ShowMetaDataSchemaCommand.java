package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;

public class ShowMetaDataSchemaCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@metadata.schema";
    }

    @Override
    public String description() {
        return "show @@metadata.schema";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("SCHEMA_NAME", JDBCType.VARCHAR)
                .addColumnInfo("DEFAULT_TARGET_NAME",JDBCType.VARCHAR)
                .addColumnInfo("TABLE_NAMES",JDBCType.VARCHAR);
        for (SchemaHandler value : MetadataManager.INSTANCE.getSchemaMap().values()) {
            String SCHEMA_NAME = value.getName();
            String DEFAULT_TARGET_NAME = value.defaultTargetName();
            String TABLE_NAMES = String.join(",",value.logicTables().keySet());
            builder.addObjectRowPayload(Arrays.asList(SCHEMA_NAME,DEFAULT_TARGET_NAME,TABLE_NAMES));
        }
        response.sendResultSet(()->builder.build());
    }
}
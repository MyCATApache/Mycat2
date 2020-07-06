package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;



/**
 * @ chenjunwen
 */

public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowColumnsStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowColumnsStatement ast = request.getAst();
        response.proxyShow(ast);
        return ExecuteCode.PERFORMED;
//        response.proxyShow(ast);
//        return ExecuteCode.PERFORMED;
//        String defaultSchema = dataContext.getDefaultSchema();
//        String schema = ast.getDatabase() != null ? SQLUtils.normalize(ast.getDatabase().getSimpleName()) : defaultSchema;
//        boolean full = ast.isFull();
//        String table = SQLUtils.normalize(ast.getTable().getSimpleName());
//
//        SQLSelectBuilderImpl sqlSelectBuilder = new SQLSelectBuilderImpl(DbType.mysql);
//        if (ast.getLike() != null) {
//            sqlSelectBuilder.whereAnd(" COLUMN_NAME " + ast.getLike());
//        }
//        if (ast.getWhere() != null) {
//            sqlSelectBuilder.whereAnd(ast.getWhere().toString());
//        }
//        if (full) {
//            sqlSelectBuilder.selectWithAlias("COLUMN_NAME", "Field");
//            sqlSelectBuilder.selectWithAlias("DATA_TYPE", "Type");
//            sqlSelectBuilder.selectWithAlias("COLLATION_NAME", "Collection");
//            sqlSelectBuilder.selectWithAlias("IS_NULLABLE", "Null");
//            sqlSelectBuilder.selectWithAlias("COLUMN_KEY", "Key");
//            sqlSelectBuilder.selectWithAlias("COLUMN_DEFAULT", "Default");
//            sqlSelectBuilder.selectWithAlias("EXTRA", "Extra");
//            sqlSelectBuilder.selectWithAlias("PRIVILEGES", "Privileges");
//            sqlSelectBuilder.selectWithAlias("COLUMN_COMMENT", "Comment");
//        } else {
//            sqlSelectBuilder.selectWithAlias("COLUMN_NAME", "Field");
//            sqlSelectBuilder.selectWithAlias("DATA_TYPE", "Type");
//            sqlSelectBuilder.selectWithAlias("IS_NULLABLE", "Null");
//            sqlSelectBuilder.selectWithAlias("COLUMN_KEY", "key");
//            sqlSelectBuilder.selectWithAlias("COLUMN_DEFAULT", "Default");
//            sqlSelectBuilder.selectWithAlias("EXTRA", "Extra");
//        }
//        String sql = sqlSelectBuilder.from("information_schema.COLUMNS").toString();
//
//        SchemaObject table1 = MetadataManager.INSTANCE.TABLE_REPOSITORY.findTable(schema + "." + table);
//        if (table1 == null) {
//            response.proxyShow(ast);
//            return ExecuteCode.PERFORMED;
//        }
//        InformationSchema informationSchema = InformationSchemaRuntime.INSTANCE.get();
//        TableHandler tableHandler = MetadataManager.INSTANCE.getTable(schema, table);
//        List<InformationSchema.COLUMNS_TABLE_OBJECT> array = new ArrayList<>();
//        long index = 0;
//        for (SimpleColumnInfo column : tableHandler.getColumns()) {
//            SQLColumnDefinition table1Column = table1.findColumn(column.getColumnName());
//            String columnName = SQLUtils.normalize(column.getColumnName());
//            InformationSchema.COLUMNS_TABLE_OBJECT.COLUMNS_TABLE_OBJECTBuilder builder
//                    = InformationSchema
//                    .COLUMNS_TABLE_OBJECT
//                    .builder()
//                    .TABLE_CATALOG("def")
//                    .TABLE_SCHEMA(schema)
//                    .TABLE_NAME(table)
//                    .COLUMN_NAME(columnName)
//                    .ORDINAL_POSITION(index);
//            if (table1Column.getDefaultExpr() != null) {
//                builder.COLUMN_DEFAULT(table1Column.getDefaultExpr().toString());
//            }
//
//            builder.IS_NULLABLE(column.isNullable() ? "YES" : "NO");
//            builder.DATA_TYPE(table1Column.getDataType().toString());
//
//            array.add(builder.build());
//            index++;
//        }
//        informationSchema.COLUMNS = array.toArray(new InformationSchema.COLUMNS_TABLE_OBJECT[0]);
//        MycatDBClientMediator client = MycatDBs.createClient(dataContext, new MycatDBClientBasedConfig(MetadataManager.INSTANCE.getSchemaMap()
//                , Collections.singletonMap("information_schema", InformationSchemaRuntime.INSTANCE.get()
//        ), false));
//        ;
//
//        response.sendResultSet(() -> client.query(sql), () -> {
//            throw new UnsupportedOperationException();
//        });
//        return ExecuteCode.PERFORMED;
    }
}

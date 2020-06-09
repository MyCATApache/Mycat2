package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.fastsql.sql.builder.impl.SQLSelectBuilderImpl;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mysql.InformationSchema;
import io.mycat.beans.mysql.InformationSchemaRuntime;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.TableHandler;
import io.mycat.queryCondition.SimpleColumnInfo;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBClientBasedConfig;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ chenjunwen
 */
@Resource
public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowColumnsStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowColumnsStatement ast = request.getAst();
//        response.proxyShow(ast);
//        return ExecuteCode.PERFORMED;
        String defaultSchema = dataContext.getDefaultSchema();
        String schema = ast.getDatabase() != null ? SQLUtils.normalize(ast.getDatabase().getSimpleName()) : defaultSchema;
        boolean full = ast.isFull();
        String table = SQLUtils.normalize(ast.getTable().getSimpleName());

        SQLSelectBuilderImpl sqlSelectBuilder = new SQLSelectBuilderImpl(DbType.mysql);
        if (ast.getLike() != null) {
            sqlSelectBuilder.whereAnd(" COLUMN_NAME " + ast.getLike());
        }
        if (ast.getWhere() != null) {
            sqlSelectBuilder.whereAnd(ast.getWhere().toString());
        }
        if (full) {
            sqlSelectBuilder.selectWithAlias("COLUMN_NAME", "Field");
            sqlSelectBuilder.selectWithAlias("DATA_TYPE", "Type");
            sqlSelectBuilder.selectWithAlias("COLLATION_NAME", "Collection");
            sqlSelectBuilder.selectWithAlias("IS_NULLABLE", "Null");
            sqlSelectBuilder.selectWithAlias("COLUMN_KEY", "Key");
            sqlSelectBuilder.selectWithAlias("COLUMN_DEFAULT", "Default");
            sqlSelectBuilder.selectWithAlias("EXTRA", "Extra");
            sqlSelectBuilder.selectWithAlias("PRIVILEGES", "Privileges");
            sqlSelectBuilder.selectWithAlias("COLUMN_COMMENT", "Comment");
        } else {
            sqlSelectBuilder.selectWithAlias("COLUMN_NAME", "Field");
            sqlSelectBuilder.selectWithAlias("DATA_TYPE", "Type");
            sqlSelectBuilder.selectWithAlias("IS_NULLABLE", "Null");
            sqlSelectBuilder.selectWithAlias("COLUMN_KEY", "key");
            sqlSelectBuilder.selectWithAlias("COLUMN_DEFAULT", "Default");
            sqlSelectBuilder.selectWithAlias("EXTRA", "Extra");
        }
        String sql = sqlSelectBuilder.from("information_schema.COLUMNS").toString();

        SchemaObject table1 = MetadataManager.INSTANCE.TABLE_REPOSITORY.findTable(schema + "." + table);
        if (table1 == null) {
            response.proxyShow(ast);
            return ExecuteCode.PERFORMED;
        }
        InformationSchema informationSchema = InformationSchemaRuntime.INSTANCE.get();
        TableHandler tableHandler = MetadataManager.INSTANCE.getTable(schema, table);
        List<InformationSchema.COLUMNS_TABLE_OBJECT> array = new ArrayList<>();
        long index = 0;
        for (SimpleColumnInfo column : tableHandler.getColumns()) {
            SQLColumnDefinition table1Column = table1.findColumn(column.getColumnName());
            String columnName = SQLUtils.normalize(column.getColumnName());
            InformationSchema.COLUMNS_TABLE_OBJECT.COLUMNS_TABLE_OBJECTBuilder builder
                    = InformationSchema
                    .COLUMNS_TABLE_OBJECT
                    .builder()
                    .TABLE_CATALOG("def")
                    .TABLE_SCHEMA(schema)
                    .TABLE_NAME(table)
                    .COLUMN_NAME(columnName)
                    .ORDINAL_POSITION(index);
            if (table1Column.getDefaultExpr() != null) {
                builder.COLUMN_DEFAULT(table1Column.getDefaultExpr().toString());
            }

            builder.IS_NULLABLE(column.isNullable() ? "YES" : "NO");
            builder.DATA_TYPE(table1Column.getDataType().toString());

            array.add(builder.build());
            index++;
        }
        informationSchema.COLUMNS = array.toArray(new InformationSchema.COLUMNS_TABLE_OBJECT[0]);
        MycatDBClientMediator client = MycatDBs.createClient(dataContext, new MycatDBClientBasedConfig(MetadataManager.INSTANCE.getSchemaMap()
                , Collections.singletonMap("information_schema", InformationSchemaRuntime.INSTANCE.get()
        ), false));
        ;

        response.sendResultSet(()->client.query(sql),()->{throw new UnsupportedOperationException();});
        return ExecuteCode.PERFORMED;
    }
}

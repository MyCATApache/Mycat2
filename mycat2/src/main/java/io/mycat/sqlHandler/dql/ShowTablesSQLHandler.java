package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.MycatDataContext;
import io.mycat.beans.mysql.InformationSchema.TABLES_TABLE_OBJECT;
import io.mycat.beans.mysql.InformationSchemaRuntime;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.TableHandler;
import io.mycat.router.ShowStatementRewriter;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Resource
public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) {

        List<TableHandler> collect = MetadataManager.INSTANCE.getSchemaMap().values().stream().distinct()
                .flatMap(i -> i.logicTables().values().stream()).distinct().collect(Collectors.toList());
        ArrayList<TABLES_TABLE_OBJECT> objects = new ArrayList<>();
        for (TableHandler value : collect) {
            String TABLE_CATALOG = "def";
            String TABLE_SCHEMA = value.getSchemaName();
            String TABLE_NAME = value.getTableName();
            String TABLE_TYPE = "BASE TABLE";
            String ENGINE = "InnoDB";
            Long VERSION = 10L;
            String ROW_FORMAT = "DYNAMIC";
            TABLES_TABLE_OBJECT tableObject = TABLES_TABLE_OBJECT.builder()
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
        TABLES_TABLE_OBJECT[] tables_table_objects = objects.toArray(new TABLES_TABLE_OBJECT[0]);
        InformationSchemaRuntime.INSTANCE.update(informationSchema -> informationSchema.TABLES = tables_table_objects);
        String sql = ShowStatementRewriter.rewriteShowTables( dataContext.getDefaultSchema(),request.getAst());
        response.sendResultSet(MycatDBs.createClient(dataContext).query(sql), null);
        return ExecuteCode.PERFORMED;
    }
}

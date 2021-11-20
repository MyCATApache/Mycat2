package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.prototypeserver.mysql.MySQLResultSet;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;

import static io.mycat.prototypeserver.mysql.PrototypeService.getShowCreateDatabaseColumns;

public class ShowCreateDatabaseHandler extends AbstractSQLHandler<MySqlShowCreateDatabaseStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowCreateDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        List<ColumnDefPacket> columnDefPacketList = getShowCreateDatabaseColumns();
        MySQLResultSet mySQLResultSet = MySQLResultSet.create(columnDefPacketList);

        String database = SQLUtils.normalize(request.getAst().getDatabase().toString());
        SQLCreateDatabaseStatement sqlCreateDatabaseStatement = new SQLCreateDatabaseStatement();
        sqlCreateDatabaseStatement.setDatabase(database);
        ArrayList<Object[]> objects = new ArrayList<>();
        objects.add(new Object[]{database, sqlCreateDatabaseStatement.toString()});
        mySQLResultSet.setRows(objects);
        return response.sendResultSet(mySQLResultSet.build());
    }
}

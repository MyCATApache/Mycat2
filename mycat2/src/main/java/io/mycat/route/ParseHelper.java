package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLLimit;
import com.alibaba.fastsql.sql.ast.SQLOrderBy;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQuery;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.DataNode;
import io.mycat.hbt.ast.base.OrderItem;
import io.mycat.metadata.GlobalTableHandler;
import io.mycat.metadata.MetadataManager;
import io.mycat.TableHandler;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface ParseHelper {
    default public Long concertLimitGetOffset(SQLLimit limitInfo) {
        SQLExpr offset = limitInfo.getOffset();
        if (offset == null) return 0L;
        if (offset instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) offset).getNumber().longValue();
        }
        return null;
    }

    default public Long concertLimitGetLimit(SQLLimit limitInfo) {
        SQLExpr rowCount = limitInfo.getRowCount();
        if (rowCount == null) return 0L;
        if (rowCount instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) rowCount).getNumber().longValue();
        }
        return null;
    }

    default public void changeSchemaTable(SQLExprTableSource tableSource, DataNode dataNode) {
        tableSource.setSchema(dataNode.getSchema());
        tableSource.setSimpleName(dataNode.getTable());
    }

    default public MySqlSelectQueryBlock unWapperToQueryBlock(SQLStatement sqlStatement) {
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) sqlStatement).getSelect().getQuery();
            if (query instanceof MySqlSelectQueryBlock) {
                return (MySqlSelectQueryBlock) query;
            }
        }
        return null;
    }

    default public List<OrderItem> concertOrder(SQLOrderBy orderBy) {
        return null;
    }

    default Set<DataNode> getGlobalRange(SQLExprTableSource leftTable ){
        TableHandler table = MetadataManager.INSTANCE.getTable(leftTable.getSchema(), leftTable.getTableName());
        if (table!=null){
            if( table  instanceof GlobalTableHandler){
               return  ((GlobalTableHandler) table).getDataNodeMap().values().stream().map(i->i).collect(Collectors.toSet());
            }
        }
        return null;
    }
    default  public boolean isGlobal(String schema, String tableName){
        TableHandler table = MetadataManager.INSTANCE.getTable(schema, tableName);
        if (table!=null){
            return table  instanceof GlobalTableHandler;
        }
        return false;
    }

}
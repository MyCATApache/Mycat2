package io.mycat.calcite.physical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.FastSqlUtils;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

@Getter
public class MycatRouteInsertCore implements Serializable {
    private final int finalAutoIncrementIndex;
    private final List<Integer> shardingKeys;
    private transient String[] columnNames;
    private final String sql;
    private final String schemaName;
    private final String tableName;

    private transient MySqlInsertStatement mySqlInsertStatement;

    public MycatRouteInsertCore(int finalAutoIncrementIndex, List<Integer> shardingKeys, String sql, String schemaName, String tableName) {
        this.finalAutoIncrementIndex = finalAutoIncrementIndex;
        this.shardingKeys = shardingKeys;
        this.sql = sql;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public MySqlInsertStatement getMySqlInsertStatement() {
        if (mySqlInsertStatement == null) {
            mySqlInsertStatement = (MySqlInsertStatement) SQLUtils.parseSingleMysqlStatement(sql);
        }
        return FastSqlUtils.clone(mySqlInsertStatement);
    }

    public ShardingTableHandler logicTable() {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        return (ShardingTableHandler) tableHandler;
    }

    public String[] getColumnNames() {
        if (this.columnNames == null) {
            List<SQLIdentifierExpr> columns = (List) getMySqlInsertStatement().getColumns();
            this.columnNames = columns.stream().map(i -> i.normalizedName()).toArray(size -> new String[size]);
        }
        return columnNames;
    }
}

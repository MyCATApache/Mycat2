package io.mycat.calcite.physical;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.Partition;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.router.ShardingTableHandler;
import lombok.Data;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Data
public class MycatRouteUpdateCore implements Serializable {
    transient SQLStatement sqlStatement;
    final String schemaName;
    final String tableName;
    final boolean global;
    final RexNode conditions;
    final String sql;

    public MycatRouteUpdateCore(SQLStatement sqlStatement, String schemaName, String tableName, boolean global, RexNode conditions) {
        this.sqlStatement = sqlStatement;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.global = global;
        this.conditions = conditions;
        this.sql = sqlStatement.toString();
    }

    public SQLStatement getSqlStatement() {
        if (sqlStatement == null) {
            sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        }
        return sqlStatement;
    }

    @Nullable
    private List<Partition> getDataNodes(TableHandler table) {
        List<Partition> backends = null;
        switch (table.getType()) {
            case SHARDING:
                backends = ((ShardingTableHandler) table).dataNodes();
                break;
            case GLOBAL:
                backends = ((GlobalTableHandler) table).getGlobalDataNode();
                break;
            case NORMAL:
                backends = Collections.singletonList(((NormalTableHandler) table).getDataNode());
                break;
            case CUSTOM:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + table.getType());
        }
        return backends;
    }

    public ExplainWriter explain(ExplainWriter writer) {
        TableHandler table = MetaClusterCurrent.wrapper(MetadataManager.class).getTable(schemaName, tableName);
        writer.name("MycatUpdateRel").into();
        writer.item("sql", sqlStatement);
        writer.item("dataNodes", getDataNodes(table));
        return writer.ret();
    }

    public RelWriter explainTerms(RelWriter pw) {
        TableHandler table = MetaClusterCurrent.wrapper(MetadataManager.class).getTable(schemaName, tableName);
        pw.item("sql", sqlStatement + "\n");
        int index = 0;
        for (Partition partition : getDataNodes(table)) {
            pw.item("dataNodes$" + index, partition + "\n");
        }
        return pw;
    }
}

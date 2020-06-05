package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.DrdsRemoveDDLJob;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.DataNode;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ParseContext implements ParseHelper {
    private String defaultSchema;
    final SQLStatement sqlStatement;
    List<SQLExprTableSource> leftTables;
    Schema plan;
    List<SQLAggregateExpr> aggregateExprs = new LinkedList<>();
    List<SQLTableSource> complexTableSources = new LinkedList<>();
    public ParseContext(String defaultSchema,SQLStatement sqlStatement) {
        this.defaultSchema = defaultSchema;
        this.sqlStatement = sqlStatement;
    }

    public static ParseContext of(String defaultSchema,SQLStatement sqlStatement) {
        return new ParseContext(defaultSchema,sqlStatement);
    }


    public SQLStatement getSqlStatement() {
        return sqlStatement;
    }

    public MySqlSelectQueryBlock tryGetQueryBlock() {
        return unWapperToQueryBlock(getSqlStatement());
    }

    public List<SQLExprTableSource> startAndGetLeftTables() {
        if (leftTables == null) {
            SQLStatement sqlStatement = getSqlStatement();
            leftTables = new ArrayList<>();
            aggregateExprs = new ArrayList<>();
            sqlStatement.accept(new MySqlASTVisitorAdapter() {


                @Override
                public boolean visit(SQLAggregateExpr x) {
                    aggregateExprs.add(x);
                    return super.visit(x);
                }

                @Override
                public boolean visit(SQLJoinTableSource x) {
                    complexTableSources.add(x);
                    return true;
                }

                @Override
                public boolean visit(SQLSubqueryTableSource x) {
                    complexTableSources.add(x);
                    return true;
                }
                @Override
                public boolean visit(SQLUnionQueryTableSource x) {
                    complexTableSources.add(x);
                    return true;
                }

                @Override
                public boolean visit(SQLValuesTableSource x) {
                    complexTableSources.add(x);
                    return true;
                }



                @Override
                public boolean visit(SQLUnnestTableSource x) {
                    complexTableSources.add(x);
                    return true;
                }

            });
        }
        return leftTables;
    }

    public String getDefaultTarget() {
        return ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
    }


    public SharingTableInfo getSharingTableInfo(String schemaName, String tableName) {
        return null;
    }


    public SharingTableInfo getSharingTableInfo(SQLExprTableSource sqlExprTableSource) {
        return null;
    }

    public boolean isNoAgg() {
        return aggregateExprs.isEmpty();
    }

    public boolean isNoWhere() {
        return false;
    }

    public boolean isNoSubQuery() {
        return false;
    }


    public List<DataNode> computeWhere(SQLExpr where) {
        return null;
    }

    public boolean isNoGroupBy() {
        return false;
    }


    public void plan(Schema build) {
        this.plan = build;
    }

    public Schema getPlan() {
        return plan;
    }

    public boolean isNoComplexDatasource() {
        return complexTableSources.isEmpty();
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public boolean isNoOrder() {
        return false;
    }
}
package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.DataNode;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ParseContext implements ParseHelper {
    final SQLStatement sqlStatement;
    List<SQLExprTableSource> leftTables;
    Schema plan;

    public ParseContext(SQLStatement sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public  static ParseContext of(SQLStatement sqlStatement) {
        return new ParseContext(sqlStatement);
    }


    public SQLStatement getSqlStatement() {
        return sqlStatement;
    }

    public List<SQLExprTableSource> getLeftTables() {
        if (leftTables == null) {
            SQLStatement sqlStatement = getSqlStatement();
            leftTables = new ArrayList<>();
            sqlStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    if (x.getExpr() instanceof SQLName) {//必须是名字
                        leftTables.add(x);
                        return true;
                    } else {
                        return super.visit(x);
                    }
                }
            });
        }
        return leftTables;
    }

    public String getDefaultTarget() {
        return ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
    }





    public SharingTableInfo getSharingTableInfo(String schemaName, String tableName){
        return null;
    }



    public SharingTableInfo getSharingTableInfo(SQLExprTableSource sqlExprTableSource){
        return null;
    }

    public boolean isNoAgg(){
        return false;
    }

    public boolean isNoWhere(){
        return false;
    }

    public boolean isNoSubQuery(){
        return false;
    }


    public List<DataNode> computeWhere(SQLExpr where){
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
}
package io.mycat.sqlparser.util;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.Collections;
import java.util.List;

public class PrepareStmtInfo {
    final List<SQLStatement> statement;
    final int count;

    public PrepareStmtInfo(String sql) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        Counter counter = new Counter();
        sqlStatement.accept(counter);
        this.count = counter.count;
        this.statement = Collections.singletonList(SQLUtils.parseSingleMysqlStatement(sql));
    }

    public String accept(List<Object> parameters) {
        return SQLUtils.toSQLString(statement, DbType.mysql, parameters, null);
    }

    public static void main(String[] args) {
        PrepareStmtInfo prepareStmtInfo = new PrepareStmtInfo("select * from travelrecord where id =?");
        String s = prepareStmtInfo.accept( Collections.singletonList(1));

    }
    static class Counter extends MySqlASTVisitorAdapter{
        int count = 0;
        @Override
        public void endVisit(SQLVariantRefExpr x) {
            count++;
            super.endVisit(x);
        }
    }
}
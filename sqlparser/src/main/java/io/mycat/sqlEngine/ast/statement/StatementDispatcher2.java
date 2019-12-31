package io.mycat.sqlEngine.ast.statement;

import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.Collections;
import java.util.List;

public class StatementDispatcher2 extends MySqlASTVisitorAdapter {
    boolean update = true;
    final NotifyCallback notifyCallback;

    public StatementDispatcher2(NotifyCallback notifyCallback) {
        this.notifyCallback = notifyCallback;
    }

    void setUpdate(boolean update) {
        this.update = this.update || update;
    }

    void setUpdate() {
        setUpdate(true);
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        SQLSelectQueryBlock queryBlock = x.getSelect().getQueryBlock();
        setUpdate(queryBlock.isForUpdate());
        return false;
    }

    @Override
    public boolean visit(SQLUseStatement x) {
        notifyCallback.useSchema(x.getDatabase().getSimpleName());
        return false;
    }

    @Override
    public boolean visit(MySqlSetTransactionStatement x) {
        Boolean global = x.getGlobal();
        if (global) {
            throw new UnsupportedOperationException("unsupport global isolation level");
        }
        Boolean session = x.getSession();
        if (!session) {
            throw new UnsupportedOperationException("unsupport global isolation level");
        }
        String isolationLevel = x.getIsolationLevel();
        notifyCallback.setSessionIsolationLevel(isolationLevel);
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLSetStatement x) {
        List<SQLAssignItem> list = x.getItems() == null ? Collections.emptyList() : x.getItems();
        for (SQLAssignItem sqlAssignItem : list) {
            String target = sqlAssignItem.getTarget().toString().trim().toLowerCase();
            String value = sqlAssignItem.getValue().toString().trim().toLowerCase();
            switch (target) {
                case "autocommit": {
                    switch (value) {
                        case "1":
                        case "on":
                            notifyCallback.setAutocommit(true);
                        case "0":
                        case "off":
                            notifyCallback.setAutocommit(false);
                    }
                    break;
                }
                case "names": {
                    notifyCallback.setNamesCharset(value);
                    break;
                }
                case "character_set_results": {
                    notifyCallback.setCharacterSetResultsCharset(value);
                    break;
                }
            }
        }

        return super.visit(x);
    }
}
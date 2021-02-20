package io.mycat.sqlhandler.dml;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import io.mycat.MySQLVariablesUtil;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;

import java.util.Collections;
import java.util.List;


public class SetSQLHandler extends AbstractSQLHandler<SQLSetStatement> {

    static enum VarType {
        USER,
        SESSION,
        GLOABL
    }

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSetStatement> request, MycatDataContext dataContext, Response response){
        try {
            List<SQLAssignItem> items = request.getAst().getItems();
            if (items == null) {
                items = Collections.emptyList();
            }

            for (SQLAssignItem item : items) {
                VarType varType = VarType.SESSION;
                String name;
                if (item.getTarget() instanceof SQLPropertyExpr) {
                    SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) item.getTarget();
                    SQLExpr owner = sqlPropertyExpr.getOwner();
                    name = SQLUtils.normalize(sqlPropertyExpr.getSimpleName());
                    if (owner instanceof SQLVariantRefExpr) {
                        varType = ((SQLVariantRefExpr) owner).isGlobal() ? VarType.GLOABL : VarType.SESSION;
                    }
                } else {
                    name = SQLUtils.normalize(item.getTarget().toString());
                    if (name.startsWith("@@")) {
                        name = name.substring(2);
                        varType = VarType.SESSION;
                    }
                    if (name.startsWith("@")) {
                        name = name.substring(1);
                        varType = VarType.USER;
                    }
                }
                if (varType == VarType.GLOABL) {
                    throw new IllegalArgumentException("unsupported set global variables:" + item);
                }
                SQLExpr valueExpr = item.getValue();
                Object value;
                if (valueExpr instanceof SQLNullExpr) {
                    value = null;
                } else {
                    if(valueExpr instanceof SQLIdentifierExpr){
                        value=SQLUtils.normalize(((SQLIdentifierExpr) valueExpr).getSimpleName());
                    }else if(valueExpr instanceof SQLDefaultExpr) {
                        //todo
                        value = "default";
                    }else {
                        value = SQLEvalVisitorUtils.eval(DbType.mysql, valueExpr);
                    }
                }
                switch (varType) {
                    case SESSION: {
                        if ("autocommit".equalsIgnoreCase(name)) {
                            int i = MySQLVariablesUtil.toInt(value);
                            if (i == 0) {
                                if (dataContext.isInTransaction()) {
                                    dataContext.setAutoCommit(false);
                                    return response.sendOk();
                                } else {
                                    dataContext.setAutoCommit(false);
                                    return response.sendOk();
                                }
                            } else if (i == 1) {
                                if (dataContext.isInTransaction()) {
                                    dataContext.setAutoCommit(true);
                                    return response.commit();
                                } else {
                                    dataContext.setAutoCommit(true);
                                    return response.sendOk();
                                }
                            }
                        }
                        dataContext.setVariable(name, item.getValue());
                        return response.sendOk();
                    }
                    case USER:
                    case GLOABL:
                    default:
                        return response.sendError(new IllegalStateException("Unexpected value: " + varType));
                }
            }
            return response.sendOk();
        }catch (Throwable throwable){
            return response.sendError(throwable);
        }
    }

    public SetSQLHandler() {
    }

    public SetSQLHandler(Class statementClass) {
        super(statementClass);
    }
}
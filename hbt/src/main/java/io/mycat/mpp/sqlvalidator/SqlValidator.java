package io.mycat.mpp.sqlvalidator;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

public class SqlValidator {
    /**
     * 1.统一VariableRef的语法对象,便于后续变量查找
     * 2.消除*
     *
     * @param queryBlock
     * @return
     */
    public List<SQLSelectItem> expandStar(SQLSelectQueryBlock queryBlock) {
        ArrayList<SQLSelectItem> list = new ArrayList<>();
        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        for (SQLSelectItem sqlSelectItem : selectList) {
            list.add(expandSelectItem(sqlSelectItem,queryBlock));
        }
        return list;
    }

    /**
     * "*" or "TABLE.*"
     * @param sqlSelectItem
     * @param queryBlock
     * @return
     */
    public SQLSelectItem expandSelectItem(SQLSelectItem sqlSelectItem, SQLSelectQueryBlock queryBlock) {
        final SelectScope  scope = getWhereScope( queryBlock);
        return null;
    }

    private SelectScope getWhereScope(SQLSelectQueryBlock queryBlock) {
        return null;
    }

    public static class SelectScope{

    }
}
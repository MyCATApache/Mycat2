package io.mycat.util;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.fastsql.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.fastsql.sql.visitor.SQLASTVisitorAdapter;
import io.mycat.DataNode;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * 真正要发给后端的更新语句
 * 用于把发送SQL过程中的变量数据内聚到一起， 不需要开发者从各个地方寻找数据。
 * @param <T>
 * @author wangzihaogithub 2020-12-29
 */
@Getter
public class UpdateSQL<T extends SQLUpdateStatement> extends SQL<T> {
    private final SQLExprTableSource tableSource;
    public static final String ATTR_TABLE_INFO = "TABLE_INFO#"+TableHandler.class.getCanonicalName();
    public static final String ATTR_COLUMN_INFO = "COLUMN_INFO#"+SimpleColumnInfo.class.getCanonicalName();
    public static final String ATTR_COLUMN_VALUE = "COLUMN_VALUE#Object";

    private final Map<SimpleColumnInfo,Object> setColumnMap = new LinkedHashMap<>();
    /**
     * 根据OR关键词分割 WHERE条件。
     * 分隔后的结构为: (ID=1 AND O_ID != 1) OR (ID = 2)
     * 为了实现判断是否是主键覆盖 {@link #isWherePrimaryKeyCovering()} {@link #getWherePrimaryKeyList()}
     */
    private final List<OrGroup> whereColumnList = new ArrayList<>();

    public UpdateSQL(String parameterizedSql, DataNode dataNode,T statement, List<Object> parameters) {
        super(parameterizedSql, dataNode, statement, parameters);
        this.tableSource = (SQLExprTableSource) statement.getTableSource();
        /**
         * 1. 给语法树的节点，绑定上列和值
         *      {@link SQLObject#putAttribute(String, Object)}
         *      {@link #ATTR_TABLE_INFO} {@link #ATTR_COLUMN_INFO} {@link #ATTR_COLUMN_VALUE}
         *
         * 2. 用于初始化条件列 {@link #setColumnMap}
         */
        statement.accept(new AttributeVisitor());

        /**
         * 根据OR关键词分割 WHERE条件, 初始化 {@link #whereColumnList}
         * 分隔后的结构为: (ID=1 AND O_ID != 1) OR (ID = 2)
         */
        WhereVisitor whereVisitor = new WhereVisitor();
        statement.getWhere().accept(whereVisitor);
        whereVisitor.end();
    }

    /**
     * 是否修改了索引中存储的字段 (索引中存储了, [1.主键, 2.索引键, 3.索引覆盖列, 4.分片地址])
     * @return true=是
     */
    public boolean isUpdateIndex(){
        for (SimpleColumnInfo columnInfo : setColumnMap.keySet()) {
            if(columnInfo.isPrimaryKey()
                    || columnInfo.isIndexKey()
                    || columnInfo.isIndexCovering()
                    || columnInfo.isShardingKey()){
                return true;
            }
        }
        return false;
    }

    /**
     * 是否修改了分片键
     * @return true=是
     */
    public boolean isUpdateShardingKey(){
        for (SimpleColumnInfo columnInfo : setColumnMap.keySet()) {
            if(columnInfo.isShardingKey()){
                return true;
            }
        }
        return false;
    }

    /**
     * 集合中每个Map是 或的关系. 例: (ID=1 OR ID = 2)
     * @return 主键集合
     */
    public Collection<Map<SimpleColumnInfo, Object>> getWherePrimaryKeyList(){
        List<Map<SimpleColumnInfo, Object>> primaryKeyList = new ArrayList<>();
        for (OrGroup orGroup : whereColumnList) {
            Map<SimpleColumnInfo, Object> pkMap = null;
            for (SQLBinaryOpExpr binaryOpExpr : orGroup.getBinaryOperatorList()) {
                if (binaryOpExpr.getOperator() != SQLBinaryOperator.Equality) {
                    continue;
                }
                SimpleColumnInfo columnInfo = (SimpleColumnInfo) binaryOpExpr.getAttribute(ATTR_COLUMN_INFO);
                if (columnInfo.isPrimaryKey()) {
                    Object columnValue = binaryOpExpr.getAttribute(ATTR_COLUMN_VALUE);
                    if(pkMap == null){
                        pkMap = new LinkedHashMap<>();
                    }
                    pkMap.put(columnInfo,columnValue);
                }
            }
            if(pkMap != null){
                primaryKeyList.add(pkMap);
            }
        }
        return primaryKeyList;
    }

    /**
     * where条件是否满足主键覆盖
     * @return true=满足主键覆盖
     */
    public boolean isWherePrimaryKeyCovering(){
        TableHandler table = getTable();
        List<SimpleColumnInfo> primaryKeyList = table.getPrimaryKeyList();
        for (OrGroup orGroup : whereColumnList) {
            Set<SimpleColumnInfo> fullSet = new HashSet<>(primaryKeyList);
            for (SQLBinaryOpExpr binaryOpExpr : orGroup.getBinaryOperatorList()) {
                if(binaryOpExpr.getOperator() == SQLBinaryOperator.Equality){
                    SimpleColumnInfo columnInfo = (SimpleColumnInfo) binaryOpExpr.getAttribute(ATTR_COLUMN_INFO);
                    if(columnInfo.isPrimaryKey()){

                    }
                    fullSet.remove(columnInfo);
                }
            }
            if(fullSet.size() > 0){
                return false;
            }
        }
        return true;
    }

    public Collection<Map<SimpleColumnInfo,Object>> selectPrimaryKey(Connection connection) throws SQLException {
        SQLUpdateStatement updateStatement = getStatement();
        TableHandler table = getTable();
        List<Object> parameters = getParameters();
        FastSqlUtils.Select select = FastSqlUtils.conversionToSelectSql(updateStatement, table.getPrimaryKeyList(), parameters);
        return select.executeQuery(connection);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * 用于根据OR关键词分割条件
     */
    @Getter
    public static class OrGroup{
        private final List<SQLBinaryOpExpr> binaryOperatorList = new ArrayList<>();
    }

    private class AttributeVisitor extends SQLASTVisitorAdapter{
        int index;
        TableHandler tableHandler;

        @Override
        public boolean visit(SQLExprTableSource x) {
            this.tableHandler = getMetadataManager().getTable(x.getSchema(), x.getTableName());
            x.putAttribute(ATTR_TABLE_INFO,tableHandler);
            return true;
        }

        @Override
        public void endVisit(SQLUpdateSetItem x) {
            String columnName = SQLUtils.normalize(x.getColumn().toString());
            SimpleColumnInfo columnByName = tableHandler.getColumnByName(columnName);
            x.putAttribute(ATTR_COLUMN_INFO,columnByName);

            Object parameter = x.getValue().getAttribute(ATTR_COLUMN_VALUE);
            setColumnMap.put(columnByName,parameter);
        }

        @Override
        public boolean visit(SQLVariantRefExpr x) {
            Object value = getParameters().get(index++);
            x.putAttribute(ATTR_COLUMN_VALUE,value);
            return true;
        }

        @Override
        public boolean visit(SQLIdentifierExpr x) {
            String columnName = SQLUtils.normalize(x.getSimpleName());
            SimpleColumnInfo columnByName = tableHandler.getColumnByName(columnName);
            x.putAttribute(ATTR_COLUMN_INFO,columnByName);
            return true;
        }
    }

    private class WhereVisitor extends SQLASTVisitorAdapter{
        private OrGroup orGroup;

        @Override
        public boolean visit(SQLBinaryOpExpr x) {
            if(x.getOperator() == SQLBinaryOperator.BooleanOr){
                if(orGroup != null) {
                    whereColumnList.add(orGroup);
                    orGroup = null;
                }
            }
            return super.visit(x);
        }

        private SQLExpr getValue(SQLBinaryOpExpr opExpr){
            SQLExpr right = opExpr.getRight();
            if(right instanceof SQLIdentifierExpr){
                return opExpr.getLeft();
            }else {
                return right;
            }
        }

        @Override
        public boolean visit(SQLIdentifierExpr x) {
            SimpleColumnInfo columnInfo = (SimpleColumnInfo) x.getAttribute(ATTR_COLUMN_INFO);
            SQLObject parent = x.getParent();
            if(parent instanceof SQLBinaryOpExpr){
                SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) parent;
                SQLExpr valueSQLExpr = getValue(opExpr);
                Object parameter = valueSQLExpr.getAttribute(ATTR_COLUMN_VALUE);

                if(orGroup == null){
                    orGroup = new OrGroup();
                }
                opExpr.putAttribute(ATTR_COLUMN_INFO,columnInfo);
                opExpr.putAttribute(ATTR_COLUMN_VALUE,parameter);
                orGroup.getBinaryOperatorList().add(opExpr);
            }
            return true;
        }
        public void end(){
            if(orGroup != null){
                whereColumnList.add(orGroup);
            }
        }
    }
}
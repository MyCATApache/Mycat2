package io.mycat.calcite;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.support.calcite.CalciteSqlBasicCall;
import com.alibaba.druid.support.calcite.TDDLSqlSelect;
import com.alibaba.druid.util.FnvHash;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatException;
import io.mycat.calcite.sqlfunction.datefunction.*;
import io.mycat.calcite.sqlfunction.infofunction.*;
import io.mycat.calcite.sqlfunction.mathfunction.Log2Function;
import io.mycat.calcite.sqlfunction.mathfunction.LogFunction;
import io.mycat.calcite.sqlfunction.mathfunction.RandFunction;
import io.mycat.calcite.sqlfunction.mathfunction.TruncateFunction;
import io.mycat.calcite.sqlfunction.stringfunction.*;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;

public class MycatCalciteMySqlNodeVisitor extends MySqlASTVisitorAdapter {
    static Map<Long, SqlOperator> operators = new HashMap<Long, SqlOperator>();

    static {

        List<SqlOperator> list = SqlStdOperatorTable.instance().getOperatorList();
        for (SqlOperator op : list) {
            long h = FnvHash.hashCode64(op.getName());
            if (h == FnvHash.Constants.TRIM) {
                continue;
            }
            operators.put(h, op);
        }
        operators.put(FnvHash.Constants.CEILING, SqlStdOperatorTable.CEIL);
    }

    static SqlOperator func(long hash) {
        return operators.get(hash);
    }

    private SqlNode sqlNode;
    private boolean hint = false;
    private  int qbIds;

    public MycatCalciteMySqlNodeVisitor(int qbIds) {
        this.qbIds = qbIds;
    }

    public MycatCalciteMySqlNodeVisitor() {
        this(0);
    }

    public SqlNode getSqlNode() {
        return sqlNode;
    }


    public boolean visit(SQLInsertStatement x) {
        SqlNodeList keywords = new SqlNodeList(new ArrayList<SqlNode>(), SqlParserPos.ZERO);

        SQLExprTableSource tableSource = (SQLExprTableSource) x.getTableSource();
        SqlNode targetTable = convertToSqlNode(tableSource.getExpr());

        SqlNode source;

        SQLSelect query = x.getQuery();
        if (query != null) {
            query.accept(this);
            source = sqlNode;
        } else {
            List<SQLInsertStatement.ValuesClause> valuesList = x.getValuesList();

            SqlNode[] rows = new SqlNode[valuesList.size()];
            for (int j = 0; j < valuesList.size(); j++) {

                List<SQLExpr> values = valuesList.get(j).getValues();

                SqlNode[] valueNodes = new SqlNode[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    SqlNode valueNode = convertToSqlNode(values.get(i));
                    valueNodes[i] = valueNode;
                }
                SqlBasicCall row = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
                rows[j] = row;
            }
            source = new SqlBasicCall(SqlStdOperatorTable.VALUES, rows, SqlParserPos.ZERO);
        }

        SqlNodeList columnList = x.getColumns().size() > 0
                ? convertToSqlNodeList(x.getColumns())
                : null;

        this.sqlNode = new SqlInsert(SqlParserPos.ZERO, keywords, targetTable, source, columnList);
        return false;
    }

    public boolean visit(MySqlInsertStatement x) {
        return visit((SQLInsertStatement) x);
    }

    private boolean visit(List<SQLInsertStatement.ValuesClause> valuesList) {
        boolean isBatch = false;
        List<SQLInsertStatement.ValuesClause> newValuesList = convertToSingleValuesIfNeed(valuesList);
        if (newValuesList.size() < valuesList.size()) {
            isBatch = true;
            valuesList = newValuesList;
        }

        SqlNode[] rows = new SqlNode[valuesList.size()];
        for (int j = 0; j < valuesList.size(); j++) {

            List<SQLExpr> values = valuesList.get(j).getValues();

            SqlNode[] valueNodes = new SqlNode[values.size()];
            for (int i = 0; i < values.size(); i++) {
                SqlNode valueNode = convertToSqlNode(values.get(i));
                valueNodes[i] = valueNode;
            }
            SqlBasicCall row = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
            rows[j] = row;
        }

        this.sqlNode = new SqlBasicCall(SqlStdOperatorTable.VALUES, rows, SqlParserPos.ZERO);

        return isBatch;
    }

    public boolean visit(MySqlUpdateStatement x) {

        if (x.getTableSource().getClass() != SQLExprTableSource.class) {
            throw new UnsupportedOperationException("Support single table only for SqlUpdate statement of calcite.");
        }
        SQLExprTableSource tableSource = (SQLExprTableSource) x.getTableSource();
        SqlNode targetTable = convertToSqlNode(tableSource.getExpr());

        List<SqlNode> columns = new ArrayList<SqlNode>();
        List<SqlNode> values = new ArrayList<SqlNode>();

        for (SQLUpdateSetItem item : x.getItems()) {
            columns.add(convertToSqlNode(item.getColumn()));
            values.add(convertToSqlNode(item.getValue()));
        }
        SqlNodeList targetColumnList = new SqlNodeList(columns, SqlParserPos.ZERO);
        SqlNodeList sourceExpressList = new SqlNodeList(values, SqlParserPos.ZERO);

        SqlNode condition = convertToSqlNode(x.getWhere());


        SqlIdentifier alias = null;
        if (x.getTableSource().getAlias() != null) {
            alias = new SqlIdentifier(SQLUtils.normalize(tableSource.getAlias()), SqlParserPos.ZERO);
        }

        sqlNode = new SqlUpdate(SqlParserPos.ZERO, targetTable, targetColumnList, sourceExpressList, condition, null, alias);

        return false;
    }

    public boolean visit(MySqlDeleteStatement x) {

        SQLExprTableSource tableSource = (SQLExprTableSource) x.getTableSource();
        SqlNode targetTable = convertToSqlNode(tableSource.getExpr());

        SqlNode condition = convertToSqlNode(x.getWhere());


        SqlIdentifier alias = null;
        if (x.getTableSource().getAlias() != null) {
            alias = new SqlIdentifier(SQLUtils.normalize(tableSource.getAlias()), SqlParserPos.ZERO);
        }

        sqlNode = new SqlDelete(SqlParserPos.ZERO, targetTable, condition, null, alias);

        return false;
    }

    @Override
    public boolean visit(SQLUnionQuery x) {

        SqlNode[] nodes;
        if (x.getRelations().size() > 2) {
            nodes = new SqlNode[x.getRelations().size()];
            for (int i = 0; i < x.getRelations().size(); i++) {
                nodes[i] = convertToSqlNode(x.getRelations().get(i));
            }
        } else {
            SqlNode left = convertToSqlNode(x.getLeft());
            SqlNode right = convertToSqlNode(x.getRight());

            nodes = new SqlNode[]{left, right};
        }

        //order by
        SqlNodeList orderBySqlNode = null;
        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            orderBySqlNode = convertOrderby(orderBy);
        }

        //limit
        SqlNode offset = null;
        SqlNode fetch = null;
        SQLLimit limit = x.getLimit();
        if (limit != null) {
            offset = convertToSqlNode(limit.getOffset());
            fetch = convertToSqlNode(limit.getRowCount());
        }

        SQLUnionOperator operator = x.getOperator();

        SqlNode union = null;
        switch (operator) {
            case UNION_ALL:
                union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL,
                        nodes,
                        SqlParserPos.ZERO);
                break;
            case UNION:
            case DISTINCT:
                union = new SqlBasicCall(SqlStdOperatorTable.UNION,
                        nodes,
                        SqlParserPos.ZERO);
                break;
            case INTERSECT:
                union = new SqlBasicCall(SqlStdOperatorTable.INTERSECT,
                        nodes,
                        SqlParserPos.ZERO);
                break;
            case EXCEPT:
                union = new SqlBasicCall(SqlStdOperatorTable.EXCEPT,
                        nodes,
                        SqlParserPos.ZERO);
                break;
            default:
                throw new MycatException("unsupported join type: " + operator);
        }

        if (null == orderBy && null == offset && null == fetch) {
            sqlNode = union;
        } else {
            //org/apache/calcite/calcite-core/1.23.0/calcite-core-1.23.0-sources.jar!/org/apache/calcite/sql/validate/SqlValidatorImpl.java:1353
            sqlNode = new SqlOrderBy(SqlParserPos.ZERO, union, orderBySqlNode, offset, fetch);
        }

        return false;

    }

    public boolean visit(MySqlSelectQueryBlock x) {
        return visit((SQLSelectQueryBlock) x);
    }

    public boolean visit(SQLSelectQueryBlock x) {
        SqlNodeList keywordList = null;
        List<SqlNode> keywordNodes = new ArrayList<SqlNode>(5);
        int option = x.getDistionOption();
        if (option != 0) {
            if (option == SQLSetQuantifier.DISTINCT
                    || option == SQLSetQuantifier.DISTINCTROW) {
                keywordNodes.add(SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO));
            } else if (option == SQLSetQuantifier.ALL) {
                keywordNodes.add(SqlSelectKeyword.ALL.symbol(SqlParserPos.ZERO));
            }

            keywordList = new SqlNodeList(keywordNodes, SqlParserPos.ZERO);
        }

        // select list
        List<SqlNode> columnNodes = new ArrayList<SqlNode>(x.getSelectList().size());
        for (SQLSelectItem selectItem : x.getSelectList()) {
            if (selectItem.getAlias() == null) {
                if (selectItem.getExpr() instanceof SQLAllColumnExpr) {

                } else if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                    if (!"*".equals(((SQLPropertyExpr) selectItem.getExpr()).getName())) {
                        selectItem.setAlias(SQLUtils.normalize(((SQLPropertyExpr) selectItem.getExpr()).getName()));
                    }
                } else if (selectItem.getExpr() instanceof SQLIdentifierExpr) {
                    selectItem.setAlias(SQLUtils.normalize(((SQLIdentifierExpr) selectItem.getExpr()).getName()));
                } else {
                    StringBuilder sb = new StringBuilder();
                    selectItem.output(sb);
                    selectItem.setAlias(sb.toString().replaceAll(" ", ""));
                }
            }
            SqlNode column = convertToSqlNode(selectItem);
            columnNodes.add(column);
        }

        //select item
        SqlNodeList selectList = new SqlNodeList(columnNodes, SqlParserPos.ZERO);

        //from
        SqlNode from = null;

        SQLTableSource tableSource = x.getFrom();
        if (tableSource != null) {
            from = convertToSqlNode(tableSource);
        }

        //where
        SQLExpr whereAst = x.getWhere();
        SqlNode where = convertToSqlNode(whereAst);
        if (whereAst != null) {
            if (where == null) {
                throw new IllegalArgumentException("该表达式不被支持:" + x.getWhere());
            }
        }
        //order by
        SqlNodeList orderBySqlNode = null;
        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            orderBySqlNode = convertOrderby(orderBy);
        }

        //group by
        SqlNodeList groupBySqlNode = null;
        SqlNode having = null;
        SQLSelectGroupByClause groupBys = x.getGroupBy();

        if (groupBys != null) {
            if (groupBys.getHaving() != null) {
                having = convertToSqlNode(groupBys.getHaving());
            }

            if (groupBys.getItems().size() > 0) {
                List<SqlNode> groupByNodes = new ArrayList<SqlNode>(groupBys.getItems().size());

                for (SQLExpr groupBy : groupBys.getItems()) {
                    SqlNode groupByNode = convertToSqlNode(groupBy);
                    groupByNodes.add(groupByNode);
                }
                groupBySqlNode = new SqlNodeList(groupByNodes, SqlParserPos.ZERO);
            }

            SqlInternalOperator op = null;
            if (groupBys.isWithRollUp()) {
                op = SqlStdOperatorTable.ROLLUP;
            } else if (groupBys.isWithCube()) {
                op = SqlStdOperatorTable.CUBE;
            }

            if (op != null) {
                List<SqlNode> rollupNodes = new ArrayList<SqlNode>(1);

                boolean isRow = false;
                for (SqlNode node : groupBySqlNode.getList()) {
                    if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator() == SqlStdOperatorTable.ROW) {
                        isRow = true;
                        break;
                    }
                }

                if (isRow) {
                    rollupNodes.add(op.createCall(SqlParserPos.ZERO, groupBySqlNode.toArray()));
                    groupBySqlNode = new SqlNodeList(rollupNodes, SqlParserPos.ZERO);
                } else {
                    rollupNodes.add(op.createCall(SqlParserPos.ZERO, groupBySqlNode));
                    groupBySqlNode = new SqlNodeList(rollupNodes, SqlParserPos.ZERO);
                }

            }
        }

        //limit
        SqlNode offset = null;
        SqlNode fetch = null;
        SQLLimit limit = x.getLimit();
        if (limit != null) {
            offset = convertToSqlNode(limit.getOffset());
            fetch = convertToSqlNode(limit.getRowCount());
        }

        //hints
        SqlNodeList hints = convertHints(x.getHints());

        if (orderBy != null && x.getParent() instanceof SQLUnionQuery) {
            this.sqlNode = new SqlSelect(SqlParserPos.ZERO
                    , keywordList
                    , selectList
                    , from
                    , where
                    , groupBySqlNode
                    , having
                    , null
                    , null
                    , offset
                    , fetch
                    , hints
            );
            sqlNode = new SqlOrderBy(SqlParserPos.ZERO
                    , sqlNode
                    , orderBySqlNode
                    , null
                    , fetch
            );
        } else {

            if (hints == null || SqlNodeList.isEmptyList(hints)) {
                this.sqlNode = new SqlSelect(SqlParserPos.ZERO
                        , keywordList
                        , selectList
                        , from
                        , where
                        , groupBySqlNode
                        , having
                        , null
                        , null
                        , null
                        , null, hints
                );

                if (orderBySqlNode != null && (!SqlNodeList.isEmptyList(orderBySqlNode))
                        || offset != null
                        || fetch != null
                ) {
                    sqlNode = new SqlOrderBy(SqlParserPos.ZERO
                            , sqlNode
                            , orderBySqlNode
                            , offset
                            , fetch);
                }
            } else {
                this.sqlNode = new SqlSelect(SqlParserPos.ZERO
                        , keywordList
                        , selectList
                        , from
                        , where
                        , groupBySqlNode
                        , having
                        , null
                        , orderBySqlNode
                        , offset
                        , fetch
                        , hints
                );
            }
        }


        return false;
    }

    public boolean visit(SQLTableSource x) {
        Class<?> clazz = x.getClass();
        if (clazz == SQLJoinTableSource.class) {
            visit((SQLJoinTableSource) x);
        } else if (clazz == SQLExprTableSource.class) {
            visit((SQLExprTableSource) x);
        } else if (clazz == SQLSubqueryTableSource.class) {
            visit((SQLSubqueryTableSource) x);
        } else {
            x.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        SqlNode table;
        SQLExpr expr = x.getExpr();
        SqlNodeList sqlNodes = convertHints((List) x.getHints(), x.computeAlias());
        if (expr instanceof SQLIdentifierExpr) {
            table = new SqlTableRef(SqlParserPos.ZERO, (SqlIdentifier) buildIdentifier((SQLIdentifierExpr) expr), sqlNodes);
        } else if (expr instanceof SQLPropertyExpr) {
            table = new SqlTableRef(SqlParserPos.ZERO, (SqlIdentifier) buildIdentifier((SQLPropertyExpr) expr), sqlNodes);
        } else {
            throw new MycatException("not support : " + expr);
        }


        if (x.getAlias() != null) {
            SqlIdentifier alias = new SqlIdentifier(SQLUtils.normalize(x.computeAlias()), SqlParserPos.ZERO);
            SqlBasicCall as = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{table, alias},
                    SqlParserPos.ZERO);
            sqlNode = as;
        } else {
            sqlNode = table;
        }

        return false;
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        SQLJoinTableSource.JoinType joinType = x.getJoinType();

        SqlNode left = convertToSqlNode(x.getLeft());
        SqlNode right = convertToSqlNode(x.getRight());
        SqlNode condition = convertToSqlNode(x.getCondition());

        SqlLiteral conditionType = condition == null
                ? JoinConditionType.NONE.symbol(SqlParserPos.ZERO)
                : JoinConditionType.ON.symbol(SqlParserPos.ZERO);

        if (condition == null && !x.getUsing().isEmpty()) {
            List<SQLExpr> using = x.getUsing();
            conditionType = JoinConditionType.USING.symbol(SqlParserPos.ZERO);
            condition = convertToSqlNodeList(x.getUsing());
        }

        switch (joinType) {
            case COMMA:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO, left,
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        JoinType.COMMA.symbol(SqlParserPos.ZERO), right,
                        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                        null);
                break;
            case JOIN:
            case INNER_JOIN:
                if (condition == null) {
                    this.sqlNode = new SqlJoin(SqlParserPos.ZERO
                            , left
                            , SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
                            , JoinType.COMMA.symbol(SqlParserPos.ZERO)
                            , right
                            , conditionType
                            , null);
                } else {
                    this.sqlNode = new SqlJoin(SqlParserPos.ZERO, left
                            , SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
                            , JoinType.INNER.symbol(SqlParserPos.ZERO), right
                            , conditionType
                            , condition);
                }
                break;
            case LEFT_OUTER_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        JoinType.LEFT.symbol(SqlParserPos.ZERO),
                        right,
                        conditionType,
                        condition);
                break;
            case RIGHT_OUTER_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        JoinType.RIGHT.symbol(SqlParserPos.ZERO),
                        right,
                        conditionType,
                        condition);
                break;
            case NATURAL_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                        JoinType.COMMA.symbol(SqlParserPos.ZERO),
                        right,
                        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                        null);
                break;
            case CROSS_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        JoinType.CROSS.symbol(SqlParserPos.ZERO),
                        right,
                        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                        null);
                break;
            case NATURAL_CROSS_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
                        JoinType.CROSS.symbol(SqlParserPos.ZERO),
                        right,
                        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                        null);
                break;
            case FULL_OUTER_JOIN:
                this.sqlNode = new SqlJoin(SqlParserPos.ZERO
                        , left
                        , SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
                        , JoinType.FULL.symbol(SqlParserPos.ZERO)
                        , right
                        , condition == null
                        ? JoinConditionType.NONE.symbol(SqlParserPos.ZERO)
                        : conditionType
                        , condition);
                break;
            default:
                throw new UnsupportedOperationException("unsupported : " + joinType);
        }

        return false;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        sqlNode = convertToSqlNode(x.getSelect());

        final String alias = x.getAlias();
        if (alias != null) {
            SqlIdentifier aliasIdentifier = new SqlIdentifier(SQLUtils.normalize(alias), SqlParserPos.ZERO);

            List<SQLName> columns = x.getColumns();

            SqlNode[] operands;
            if (columns.size() == 0) {
                operands = new SqlNode[]{sqlNode, aliasIdentifier};
            } else {
                operands = new SqlNode[columns.size() + 2];
                operands[0] = sqlNode;
                operands[1] = aliasIdentifier;
                for (int i = 0; i < columns.size(); i++) {
                    SQLName column = columns.get(i);
                    operands[i + 2] = new SqlIdentifier(
                            SQLUtils.normalize(column.getSimpleName()), SqlParserPos.ZERO);
                }
            }
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS, operands, SqlParserPos.ZERO);
        }

        return false;
    }

    public boolean visit(SQLUnionQueryTableSource x) {
        x.getUnion().accept(this);

        final String alias = x.getAlias();
        if (alias != null) {
            SqlIdentifier aliasIdentifier = new SqlIdentifier(SQLUtils.normalize(alias), SqlParserPos.ZERO);
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS,
                    new SqlNode[]{sqlNode, aliasIdentifier},
                    SqlParserPos.ZERO);
        }

        return false;
    }

    @Override
    public boolean visit(SQLInSubQueryExpr x) {
        SqlNode left = convertToSqlNode(x.getExpr());
        SqlBinaryOperator subOperator = SqlStdOperatorTable.IN;
        if (x.isNot()) {
            subOperator = SqlStdOperatorTable.NOT_IN;
        }
        SqlNode right = convertToSqlNode(x.subQuery);

        sqlNode = new SqlBasicCall(subOperator, new SqlNode[]{left, right}, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLCastExpr x) {
        SqlLiteral functionQualifier = null;

        SqlNode sqlNode = convertToSqlNode(x.getExpr());

        SQLDataType dataType = x.getDataType();
        String typeName = dataType.getName().toUpperCase();
        if (dataType.nameHashCode64() == FnvHash.Constants.INT) {
            typeName = "INTEGER";
        } else if (dataType.nameHashCode64() == FnvHash.Constants.NUMERIC) {
            typeName = "DECIMAL";
        }

        SqlIdentifier dataTypeNode = (SqlIdentifier) convertToSqlNode(
                new SQLIdentifierExpr(typeName));

        int scale = -1;
        int precision = -1;

        List<SQLExpr> arguments = dataType.getArguments();
        if (arguments != null && !arguments.isEmpty()) {
            scale = ((SQLNumericLiteralExpr) arguments.get(0)).getNumber().intValue();
            if (arguments.size() > 1) {
                precision = ((SQLNumericLiteralExpr) arguments.get(1)).getNumber().intValue();
            }
        }
        SqlNode sqlDataTypeSpec;
        if (typeName.equalsIgnoreCase("SIGNED")) {
            sqlDataTypeSpec = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, precision, scale, null, SqlParserPos.ZERO), SqlParserPos.ZERO);
        } else {
            if ("datetime".equalsIgnoreCase(typeName)) {
                typeName = "TIMESTAMP";
            } else if ("BINARY".equalsIgnoreCase(typeName)) {
                typeName = "VARCHAR";
            } else if ("UNSIGNED".equalsIgnoreCase(typeName)) {
                typeName = "DECIMAL";
            }
            SqlTypeName sqlTypeName = SqlTypeName.valueOf(typeName);
            SqlBasicTypeNameSpec sqlBasicTypeNameSpec = new SqlBasicTypeNameSpec(sqlTypeName, precision, scale, null, SqlParserPos.ZERO);

            sqlDataTypeSpec = new SqlDataTypeSpec(sqlBasicTypeNameSpec, SqlParserPos.ZERO);
        }

        SqlOperator sqlOperator = new SqlCastFunction();

        this.sqlNode = new CalciteSqlBasicCall(sqlOperator, new SqlNode[]{sqlNode, sqlDataTypeSpec}, SqlParserPos.ZERO,
                false, functionQualifier);
        return false;
    }

    public boolean visit(SQLCaseExpr x) {// CASE WHEN
        SQLExpr valueExpr = x.getValueExpr();
        SqlNode nodeValue = null;
        SqlNodeList nodeWhen = new SqlNodeList(SqlParserPos.ZERO);
        SqlNodeList nodeThen = new SqlNodeList(SqlParserPos.ZERO);
        if (valueExpr != null) {
            nodeValue = convertToSqlNode(valueExpr);
        }

        List items = x.getItems();
        int elExpr = 0;

        for (int size = items.size(); elExpr < size; ++elExpr) {
            this.visit((SQLCaseExpr.Item) items.get(elExpr));
            if (this.sqlNode != null && this.sqlNode instanceof SqlNodeList) {
                SqlNodeList nodeListTemp = (SqlNodeList) this.sqlNode;
                nodeWhen.add(nodeListTemp.get(0));
                nodeThen.add(nodeListTemp.get(1));
            }
        }
        SQLExpr elseExpr = x.getElseExpr();
        SqlNode nodeElse = convertToSqlNode(elseExpr);
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(nodeValue);
        sqlNodeList.add(nodeWhen);
        sqlNodeList.add(nodeThen);
        sqlNodeList.add(nodeElse);
        sqlNode = SqlCase.createSwitched(SqlParserPos.ZERO, nodeValue, nodeWhen, nodeThen, nodeElse);
        return false;
    }

    public boolean visit(SQLCaseExpr.Item x) {
        SQLExpr conditionExpr = x.getConditionExpr();
        SqlNode sqlNode1 = convertToSqlNode(conditionExpr);
        SQLExpr valueExpr = x.getValueExpr();
        SqlNode sqlNode2 = convertToSqlNode(valueExpr);
        SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
        sqlNodeList.add(sqlNode1);
        sqlNodeList.add(sqlNode2);
        sqlNode = sqlNodeList;
        return false;
    }

    public boolean visit(SQLListExpr x) {
        List<SQLExpr> items = x.getItems();
        List<SqlNode> objects = new ArrayList<SqlNode>();
        for (int i = 0; i < items.size(); i++) {
            SQLExpr sqlExpr = items.get(i);
            SqlNode sqlNode = convertToSqlNode(sqlExpr);
            objects.add(sqlNode);
        }
        sqlNode = SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, objects);
        return false;
    }

    @Override
    public boolean visit(SQLSelect x) {
        SQLWithSubqueryClause with = x.getWithSubQuery();
        if (with != null) {
            SqlNodeList withList = new SqlNodeList(SqlParserPos.ZERO);
            final List<SQLWithSubqueryClause.Entry> entries = with.getEntries();
            for (SQLWithSubqueryClause.Entry entry : entries) {
                visit(entry);
                withList.add(sqlNode);
            }
            SqlNode query = convertToSqlNode(x.getQuery());

            if (query instanceof SqlOrderBy) {
                SqlOrderBy orderBy = (SqlOrderBy) query;

                SqlWith w = new SqlWith(SqlParserPos.ZERO, withList, orderBy.query);
                sqlNode = new SqlOrderBy(SqlParserPos.ZERO
                        , w
                        , orderBy.orderList
                        , orderBy.offset
                        , orderBy.fetch
                );
            } else {
                sqlNode = new SqlWith(SqlParserPos.ZERO, withList, query);
            }

            if (query instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) query;
                SqlNode fetch = select.getFetch();
                SqlNodeList orderList = select.getOrderList();

                if (fetch != null
                        || (orderList != null && orderList.size() > 0)) {
                    SqlNodeList orderByList = null;
                    if (orderList != null) {
                        orderByList = new SqlNodeList(orderList.getList(), SqlParserPos.ZERO);
                        orderList.getList().clear();
                    } else {
                        orderByList = null;
                    }

                    sqlNode = new SqlOrderBy(SqlParserPos.ZERO
                            , sqlNode
                            , orderByList
                            , null
                            , fetch
                    );

                    if (fetch != null) {
                        select.setFetch(null);
                    }
                }
            }

        } else {
            sqlNode = convertToSqlNode(x.getQuery());
        }

        return false;
    }

    public boolean visit(SQLWithSubqueryClause.Entry x) {
        SqlNodeList columnList = null;
        final List<SQLName> columns = x.getColumns();
        if (columns.size() > 0) {
            columnList = new SqlNodeList(SqlParserPos.ZERO);
            for (SQLName column : columns) {
                columnList.add(new SqlIdentifier(column.getSimpleName(), SqlParserPos.ZERO));
            }
        }
        SqlNode query = convertToSqlNode(x.getSubQuery());
        SqlIdentifier name = new SqlIdentifier(SQLUtils.normalize(x.getAlias()), SqlParserPos.ZERO);
        sqlNode = new SqlWithItem(SqlParserPos.ZERO, name, columnList, query);
        return false;
    }

    @Override
    public boolean visit(SQLSelectStatement x) {

        SqlNode sqlNode = convertToSqlNode(x.getSelect());

        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;

            SqlNodeList headHints = convertHints(x.getHeadHintsDirect());
            select.setHints(headHints);
            this.sqlNode = select;
        } else {
            this.sqlNode = sqlNode;
        }


        return false;
    }

    protected void visit(SQLSelectQuery x) {
        Class<?> clazz = x.getClass();
        if (clazz == MySqlSelectQueryBlock.class) {
            visit((MySqlSelectQueryBlock) x);
        } else if (clazz == SQLUnionQuery.class) {
            visit((SQLUnionQuery) x);
        } else {
            x.accept(this);
        }
    }

    public boolean visit(SQLAllExpr x) {
        sqlNode = convertToSqlNode(x.getSubQuery());
        return false;
    }

    public boolean visit(SQLAnyExpr x) {
        sqlNode = convertToSqlNode(x.getSubQuery());
        return false;
    }

    private boolean isSqlAllExpr(SQLExpr x) {
        return x.getClass() == SQLAllExpr.class;
    }

    private boolean isAnyOrSomeExpr(SQLExpr x) {
        return x.getClass() == SQLAnyExpr.class || x.getClass() == SQLSomeExpr.class;
    }

    public boolean visit(SQLSelectItem x) {
        SQLExpr expr = x.getExpr();

        if (expr instanceof SQLIdentifierExpr) {
            visit((SQLIdentifierExpr) expr);
        } else if (expr instanceof SQLPropertyExpr) {
            visit((SQLPropertyExpr) expr);
        } else if (expr instanceof SQLAggregateExpr) {
            visit((SQLAggregateExpr) expr);
        } else {
            expr.accept(this);
        } // select a + (select count(1) from b) as mm from c;
        // select a + (select COUNT(1) from b) as 'a + (select count(1) as
        // 'count(1)' from b)' from c;
        String alias = x.getAlias();
        if (alias != null && alias.length() > 0) {
            String alias2 = x.getAlias2();
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS,
                    new SqlNode[]{sqlNode, new SqlIdentifier(SQLUtils.normalize(alias2, DbType.mysql), SqlParserPos.ZERO)},
                    SqlParserPos.ZERO);
        }

        return false;
    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        if (x.getName().equalsIgnoreCase("unknown")) {
            sqlNode = SqlLiteral.createUnknown(SqlParserPos.ZERO);
            return false;
        }
        sqlNode = buildIdentifier(x);
        return false;
    }

    public boolean visit(SQLPropertyExpr x) {
        sqlNode = buildIdentifier(x);
        return false;
    }

    SqlNode buildIdentifier(SQLIdentifierExpr x) {
        return new SqlIdentifier(SQLUtils.normalize(x.getName()), SqlParserPos.ZERO);
    }

    SqlNode buildIdentifier(SQLPropertyExpr x) {
        String name = SQLUtils.normalize(x.getName());
        if ("*".equals(name)) {
            name = "";
        }

        SQLExpr owner = x.getOwner();

        List<String> names;
        if (owner instanceof SQLIdentifierExpr) {
            names = Arrays.asList(((SQLIdentifierExpr) owner).normalizedName(), name);
        } else if (owner instanceof SQLPropertyExpr) {
            names = new ArrayList<String>();
            buildIdentifier((SQLPropertyExpr) owner, names);
            names.add(name);
        } else if (owner instanceof SQLVariantRefExpr) {
            return handleSQLVariantRefExpr(name, (SQLVariantRefExpr) owner);
        } else {
            throw new MycatException("not support : " + owner);
        }

        return new SqlIdentifier(names, SqlParserPos.ZERO);
    }

    public static SqlNode handleSQLVariantRefExpr(String name, SQLVariantRefExpr owner) {
        if (owner.isGlobal()) {
            return MycatGlobalValueFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                    SqlLiteral.createCharString(name, SqlParserPos.ZERO));
        } else {
            if (name.startsWith("@@")) {
                return MycatSessionValueFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                        SqlLiteral.createCharString(name.substring(2), SqlParserPos.ZERO));
            }
            if (name.startsWith("@")) {
                return MycatUserValueFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                        SqlLiteral.createCharString(name.substring(1), SqlParserPos.ZERO));
            }
            return MycatSessionValueFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                    SqlLiteral.createCharString(name, SqlParserPos.ZERO));
        }
    }

    void buildIdentifier(SQLPropertyExpr x, List<String> names) {
        String name = SQLUtils.normalize(x.getName());

        SQLExpr owner = x.getOwner();
        if (owner instanceof SQLIdentifierExpr) {
            names.add(((SQLIdentifierExpr) owner).normalizedName());
        } else if (owner instanceof SQLPropertyExpr) {
            buildIdentifier((SQLPropertyExpr) owner, names);
        } else {
            throw new MycatException("not support : " + owner);
        }

        names.add(name);
    }

    public boolean visit(SQLBinaryOpExprGroup x) {
        SqlOperator operator = null;
        switch (x.getOperator()) {
            case BooleanAnd:
                operator = SqlStdOperatorTable.AND;
                break;
            case BooleanOr:
                operator = SqlStdOperatorTable.OR;
                break;
            default:
                break;
        }

        final List<SQLExpr> items = x.getItems();
        SqlNode group = null;
        for (int i = 0; i < items.size(); i++) {
            SQLExpr item = items.get(i);
            final SqlNode calciteNode = convertToSqlNode(item);
            if (group == null) {
                group = calciteNode;
            } else {
                group = new SqlBasicCall(operator, new SqlNode[]{group, calciteNode}, SqlParserPos.ZERO);
                ;
            }
        }
        this.sqlNode = group;
        return false;
    }

    public boolean visit(SQLBinaryOpExpr x) {
        SqlOperator operator = null;

        SqlQuantifyOperator someOrAllOperator = null;

        SqlNode left = convertToSqlNode(x.getLeft());

        SQLExpr rightExpr = x.getRight();
        SqlNode right = convertToSqlNode(rightExpr);

        switch (x.getOperator()) {
            case Equality:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_EQ;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_EQ;
                } else {
                    operator = SqlStdOperatorTable.EQUALS;
                }
                break;
            case GreaterThan:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_GT;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_GT;
                } else {
                    operator = SqlStdOperatorTable.GREATER_THAN;
                }
                break;
            case GreaterThanOrEqual:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_GE;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_GE;
                } else {
                    operator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
                }
                break;
            case LessThan:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_LT;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_LT;
                } else {
                    operator = SqlStdOperatorTable.LESS_THAN;
                }
                break;
            case LessThanOrEqual:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_LE;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_LE;
                } else {
                    operator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
                }
                break;
            case NotEqual:
            case LessThanOrGreater:
                if (isSqlAllExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.ALL_NE;
                } else if (isAnyOrSomeExpr(rightExpr)) {
                    someOrAllOperator = SqlStdOperatorTable.SOME_NE;
                } else {
                    operator = SqlStdOperatorTable.NOT_EQUALS;
                }
                break;
            case Add:
                if (rightExpr instanceof SQLIntervalExpr || x.getLeft() instanceof SQLIntervalExpr) {
                    operator = DateAddFunction.INSTANCE;
                    break;
                }
                operator = SqlStdOperatorTable.PLUS;
                break;
            case Subtract:
                if (rightExpr instanceof SQLIntervalExpr) {
                    SQLExpr value = ((SQLIntervalExpr) rightExpr).getValue();
                    SQLIntegerExpr value1 = (SQLIntegerExpr) value;
                    right = convertToSqlNode(new SQLIntervalExpr(new SQLIntegerExpr(-value1.getNumber().longValue()),
                            ((SQLIntervalExpr) rightExpr).getUnit()));
                    operator = DateAddFunction.INSTANCE;
                    break;
                }
                operator = SqlStdOperatorTable.MINUS;
                break;
            case Union:
                operator = SqlStdOperatorTable.UNION;
                break;
            case COLLATE: {
                SQLMethodInvokeExpr convert = new SQLMethodInvokeExpr("convert", (SQLExpr) x.getParent(),
                        x.getLeft());
                convert.setUsing(x.getRight());
                convert.accept(this);
                return false;
            }
            case BitwiseXor:
                operator = SqlStdOperatorTable.BIT_XOR;
                break;
            case BitwiseXorEQ:
                break;
            case Multiply:
                operator = SqlStdOperatorTable.MULTIPLY;
                break;
            case Divide:
                operator = SqlStdOperatorTable.DIVIDE;
                break;
            case DIV:
                operator = SqlStdOperatorTable.DIVIDE_INTEGER;
                break;
            case Modulus:
                operator = SqlStdOperatorTable.MOD;
                break;
            case Like:
                operator = SqlStdOperatorTable.LIKE;
                break;
            case NotLike:
                operator = SqlStdOperatorTable.NOT_LIKE;
                break;
            case BooleanAnd:
                operator = SqlStdOperatorTable.AND;
                break;
            case BooleanOr:
                operator = SqlStdOperatorTable.OR;
                break;
            case Concat:
                operator = SqlStdOperatorTable.CONCAT;
                break;
            case Is: {
                if (rightExpr instanceof SQLNullExpr) {
                    operator = IS_NULL;
                } else if (rightExpr instanceof SQLIdentifierExpr) {
                    long hashCode64 = ((SQLIdentifierExpr) rightExpr).nameHashCode64();
                    if (hashCode64 == FnvHash.Constants.JSON
                            || hashCode64 == JSON_VALUE) {
                        operator = SqlStdOperatorTable.IS_JSON_VALUE;
                    } else if (hashCode64 == JSON_OBJECT) {
                        operator = SqlStdOperatorTable.IS_JSON_OBJECT;
                    } else if (hashCode64 == JSON_ARRAY) {
                        operator = SqlStdOperatorTable.IS_JSON_ARRAY;
                    } else if (hashCode64 == JSON_SCALAR) {
                        operator = SqlStdOperatorTable.IS_JSON_SCALAR;
                    } else if (hashCode64 == FnvHash.Constants.UNKNOWN) {
                        operator = SqlStdOperatorTable.IS_UNKNOWN;
                    }
                } else if (rightExpr instanceof SQLBooleanExpr) {
                    if (((SQLBooleanExpr) rightExpr).getValue()) {
                        operator = SqlStdOperatorTable.IS_TRUE;
                    } else {
                        operator = SqlStdOperatorTable.IS_FALSE;
                    }
                }
            }
            break;
            case IsNot:
                if (rightExpr instanceof SQLNullExpr) {
                    operator = SqlStdOperatorTable.IS_NOT_NULL;
                } else if (rightExpr instanceof SQLIdentifierExpr) {
                    long hashCode64 = ((SQLIdentifierExpr) rightExpr).nameHashCode64();
                    if (hashCode64 == FnvHash.Constants.JSON
                            || hashCode64 == JSON_VALUE) {
                        operator = SqlStdOperatorTable.IS_NOT_JSON_VALUE;
                    } else if (hashCode64 == JSON_OBJECT) {
                        operator = SqlStdOperatorTable.IS_NOT_JSON_OBJECT;
                    } else if (hashCode64 == JSON_ARRAY) {
                        operator = SqlStdOperatorTable.IS_NOT_JSON_ARRAY;
                    } else if (hashCode64 == JSON_SCALAR) {
                        operator = SqlStdOperatorTable.IS_NOT_JSON_SCALAR;
                    } else if (hashCode64 == FnvHash.Constants.UNKNOWN) {
                        operator = SqlStdOperatorTable.IS_NOT_UNKNOWN;
                    }
                } else if (rightExpr instanceof SQLBooleanExpr) {
                    if (((SQLBooleanExpr) rightExpr).getValue()) {
                        operator = SqlStdOperatorTable.IS_NOT_TRUE;
                    } else {
                        operator = SqlStdOperatorTable.IS_NOT_FALSE;
                    }
                }
                break;
            case Escape: {
                SqlBasicCall like = (SqlBasicCall) left;
                sqlNode = new SqlBasicCall(like.getOperator(), new SqlNode[]{like.operands[0], like.operands[1], right},
                        SqlParserPos.ZERO);
                return false;
            }
            case BitwiseOr: {
                sqlNode = new SqlBasicCall(new SqlUnresolvedFunction(new SqlIdentifier("|", SqlParserPos.ZERO),
                        null,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION), new SqlNode[]{left, right}, SqlParserPos.ZERO);
                return false;
            }
            case LessThanOrEqualOrGreaterThan: {
                sqlNode = new SqlBasicCall(new SqlUnresolvedFunction(new SqlIdentifier("<=>", SqlParserPos.ZERO),
                        null,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION), new SqlNode[]{left, right}, SqlParserPos.ZERO);
                return false;
            }

            case NotRegExp:
                sqlNode = NotRegexpFunction.INSTANCE.createCall(SqlParserPos.ZERO, new SqlNode[]{left, right});
                return false;
            case RLike:
            case RegExp:
                sqlNode = RegexpFunction.INSTANCE.createCall(SqlParserPos.ZERO, new SqlNode[]{left, right});
                return false;
            case SoudsLike:
                SQLBinaryOpExpr eq = SQLBinaryOpExpr.eq(
                        new SQLMethodInvokeExpr("SOUNDEX", null, x.getLeft()),
                        new SQLMethodInvokeExpr("SOUNDEX", null, x.getRight()));
                eq.accept(this);
                return false;
            case Mod:

            case SubGt:

            case SubGtGt:

            case PoundGt:

            case PoundGtGt:

            case QuesQues:

            case QuesBar:

            case QuesAmp:

            case LeftShift:

            case RightShift:

            case BitwiseAnd:

            case IsDistinctFrom:

            case IsNotDistinctFrom:


            case ILike:

            case NotILike:

            case AT_AT:

            case SIMILAR_TO:

            case POSIX_Regular_Match:

            case POSIX_Regular_Match_Insensitive:

            case POSIX_Regular_Not_Match:

            case POSIX_Regular_Not_Match_POSIX_Regular_Match_Insensitive:

            case Array_Contains:

            case Array_ContainedBy:

            case SAME_AS:

            case JSONContains:

            case NotRLike:

            case NotLessThan:

            case NotGreaterThan:


            case BitwiseNot:

            case BooleanXor:

            case Assignment:

            case PG_And:

            case PG_ST_DISTANCE:

            default:
                SqlUnresolvedFunction sqlUnresolvedFunction = new SqlUnresolvedFunction(new SqlIdentifier(x.getOperator().name(), SqlParserPos.ZERO),
                        null,
                        null,
                        null,
                        null,
                        SqlFunctionCategory.USER_DEFINED_FUNCTION) {
                    @Override
                    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
                        super.unparse(writer, call, leftPrec, rightPrec);
                    }
                };
                sqlNode = new SqlBasicCall(sqlUnresolvedFunction, new SqlNode[]{left, right}, SqlParserPos.ZERO);
                return false;

        }


        if (someOrAllOperator != null) {
            this.sqlNode = new SqlBasicCall(someOrAllOperator, new SqlNode[]{left, right},
                    SqlParserPos.ZERO);
        } else {
            if (operator == IS_NULL
                    || operator == SqlStdOperatorTable.IS_NOT_NULL
                    || operator == SqlStdOperatorTable.IS_TRUE
                    || operator == SqlStdOperatorTable.IS_NOT_TRUE) {
                this.sqlNode = new SqlBasicCall(operator,
                        new SqlNode[]{left},
                        SqlParserPos.ZERO);
            } else {
                this.sqlNode = new SqlBasicCall(operator,
                        new SqlNode[]{left, right},
                        SqlParserPos.ZERO);
            }
        }
        return false;
    }

    public boolean visit(SQLBetweenExpr x) {
        SQLExpr testExpr = x.getTestExpr();
        SqlOperator sqlOperator = SqlStdOperatorTable.BETWEEN;
        if (x.isNot()) {
            sqlOperator = SqlStdOperatorTable.NOT_BETWEEN;
        }
        SqlNode sqlNode = convertToSqlNode(testExpr);
        SqlNode sqlNodeBegin = convertToSqlNode(x.getBeginExpr());
        SqlNode sqlNodeEnd = convertToSqlNode(x.getEndExpr());
        ArrayList<SqlNode> sqlNodes = new ArrayList<SqlNode>(3);
        sqlNodes.add(sqlNode);
        sqlNodes.add(sqlNodeBegin);
        sqlNodes.add(sqlNodeEnd);
        this.sqlNode = new SqlBasicCall(sqlOperator, SqlParserUtil.toNodeArray(sqlNodes), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLExistsExpr x) {
        SqlOperator sqlOperator = SqlStdOperatorTable.EXISTS;
        SqlNode sqlNode = sqlOperator.createCall(SqlParserPos.ZERO,
                convertToSqlNode(x.getSubQuery()));
        if (x.isNot()) {
            sqlNode = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, sqlNode);
        }
        this.sqlNode = sqlNode;
        return false;
    }

    public boolean visit(SQLAllColumnExpr x) {
        sqlNode = new SqlIdentifier(Arrays.asList(""), SqlParserPos.ZERO);

        return false;
    }

    public boolean visit(SQLCharExpr x) {
        String text = x.getText();
        text = text.replaceAll("\\\\", "\\\\\\\\");
        sqlNode = SqlLiteral.createCharString(text, StandardCharsets.UTF_8.name(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLNCharExpr x) {
        String text = x.getText();
        text = text.replaceAll("\\\\", "\\\\\\\\");
        sqlNode = SqlLiteral.createCharString(text, StandardCharsets.UTF_8.name(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLNullExpr x) {
        sqlNode = SqlLiteral.createNull(SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLIntegerExpr x) {
        sqlNode = SqlLiteral.createExactNumeric(x.getNumber().toString(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLBooleanExpr x) {
        sqlNode = SqlLiteral.createBoolean(x.getBooleanValue(), SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLNumberExpr x) {
        String str = x.toString();
        if (str.indexOf('E') > 0 || str.indexOf('e') > 0) {
            sqlNode = SqlLiteral.createApproxNumeric(str, SqlParserPos.ZERO);
        } else {
            BigDecimal bigDecimal = SqlParserUtil.parseDecimal(str);
            sqlNode = MysqlExactNumericLiteral.create(bigDecimal, SqlParserPos.ZERO);
        }
        return false;
    }

    public boolean visit(SQLTimestampExpr x) {
        String literal = x.getLiteral();
        int precision = 0;
        if (literal.endsWith("00")) {
            char c3 = literal.charAt(literal.length() - 3);
            if (c3 >= '0' && c3 <= '9') {
                literal = literal.substring(0, literal.length() - 2);
                precision = 3;
            }
        }
        TimestampString ts = new TimestampString(literal);
        sqlNode = SqlLiteral.createTimestamp(ts, precision, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLDateExpr x) {
        String literal = x.getLiteral();
        DateString ds = new DateString(literal);
        sqlNode = SqlLiteral.createDate(ds, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLTimeExpr x) {
        String literal = ((SQLCharExpr) x.getLiteral()).getText();
        TimeString ds = new TimeString(literal);
        sqlNode = SqlLiteral.createTime(ds, 0, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLCurrentTimeExpr x) {
        sqlNode = new SqlIdentifier(x.getType().name, SqlParserPos.ZERO);
        return false;
    }

    public boolean visit(SQLAggregateExpr x) {
        SqlOperator functionOperator;

        String methodName = x.getMethodName();


        long hashCode64 = x.methodNameHashCode64();

        functionOperator = func(hashCode64);

        if (functionOperator == null) {
            functionOperator = new SqlUnresolvedFunction(new SqlIdentifier(methodName, SqlParserPos.ZERO),
                    null,
                    null,
                    null,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }

        SqlLiteral functionQualifier = null;

        if (x.getOption() == SQLAggregateOption.DISTINCT) {
            functionQualifier = SqlSelectKeyword.DISTINCT.symbol(SqlParserPos.ZERO);
        }
        List<SQLExpr> arguments = x.getArguments();
        List<SqlNode> argNodes = new ArrayList<SqlNode>(arguments.size());
        for (int i = 0, size = arguments.size(); i < size; ++i) {
            argNodes.add(convertToSqlNode(arguments.get(i)));
        }
        this.sqlNode = functionOperator.createCall(functionQualifier,
                SqlParserPos.ZERO,
                SqlParserUtil.toNodeArray(argNodes)
        );

        SQLOrderBy withinGroup = x.getWithinGroup();
        if (withinGroup != null) {
            SqlNodeList orderByItems = convertOrderby(withinGroup);

            this.sqlNode = SqlStdOperatorTable.WITHIN_GROUP
                    .createCall(SqlParserPos.ZERO, this.sqlNode, orderByItems);
        }

        SQLOver over = x.getOver();
        if (over != null) {
            SqlNode aggNode = this.sqlNode;
            SQLOver.WindowingBound windowingBetweenBeginBound = over.getWindowingBetweenBeginBound();
            SQLOver.WindowingBound windowingBetweenEndBound = over.getWindowingBetweenEndBound();

            boolean isRow = over.getWindowingType() != SQLOver.WindowingType.RANGE;
            SqlNode lowerBound;
            if (over.getWindowingBetweenBegin() != null) {
                over.getWindowingBetweenBegin().accept(this);
                lowerBound = SqlWindow.createPreceding(sqlNode, SqlParserPos.ZERO);
            } else {
                lowerBound = createSymbol(windowingBetweenBeginBound);
            }
            SqlNode upperBound = createSymbol(windowingBetweenEndBound);

            SqlWindow window = new SqlWindow(SqlParserPos.ZERO
                    , null
                    , null
                    , convertToSqlNodeList(over.getPartitionBy())
                    , convertOrderby(over.getOrderBy())
                    , SqlLiteral.createBoolean(isRow, SqlParserPos.ZERO)
                    , lowerBound
                    , upperBound
                    , null
            );
            sqlNode = SqlStdOperatorTable.OVER.createCall(
                    SqlParserPos.ZERO,
                    aggNode,
                    window);
        }


        SQLExpr filter = x.getFilter();
        if (filter != null) {
            SqlNode aggNode = this.sqlNode;

            filter.accept(this);
            sqlNode = SqlStdOperatorTable.FILTER.createCall(
                    SqlParserPos.ZERO,
                    aggNode,
                    sqlNode);
        }


        return false;
    }

    protected static SqlNode createSymbol(SQLOver.WindowingBound bound) {
        if (bound == null) {
            return null;
        }

        switch (bound) {
            case CURRENT_ROW:
                return SqlWindow.createCurrentRow(SqlParserPos.ZERO);
            case UNBOUNDED_FOLLOWING:
                return SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
            case UNBOUNDED_PRECEDING:
                return SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            default:
                return null;
        }
    }

    public boolean visit(SQLMethodInvokeExpr x) {
        List<SQLExpr> arguments = x.getArguments();


        if (x.getFrom() != null) {
            arguments.add(x.getFrom());
        }
        if (x.getFor() != null) {
            arguments.add(x.getFor());
        }


        List<SqlNode> argNodes = new ArrayList<SqlNode>(arguments.size());

        long nameHashCode64 = x.methodNameHashCode64();
        SqlOperator functionOperator = func(nameHashCode64);
        String methodName = x.getMethodName().toUpperCase();
        for (SQLExpr exp : arguments) {
            argNodes.add(Objects.requireNonNull(convertToSqlNode(exp)));
        }

        switch (methodName) {
            case "ASCII": {
                this.sqlNode = AsciiFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "IN": {
                this.sqlNode = SqlStdOperatorTable.IN.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "BIT_LENGTH": {
                this.sqlNode = BitLengthFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "BIN": {
                this.sqlNode = BinFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "EXPORT_SET": {
                this.sqlNode = ExportSetFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "UNHEX": {
                this.sqlNode = UnhexFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LCASE":
            case "LOWER": {
                this.sqlNode = SqlStdOperatorTable.LOWER.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SUBSTRING": {
                this.sqlNode = SubStringFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LPAD": {
                this.sqlNode = LpadFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LTRIM": {
                this.sqlNode = LtrimFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MAKE_SET": {
                this.sqlNode = MakeSetFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "DAYNAME": {
                this.sqlNode = DaynameFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "CURDATE":
            case "CURRENT_DATE": {
                this.sqlNode = CurDateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "ADDTIME": {
                this.sqlNode = AddTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TRUNCATE": {
                this.sqlNode = TruncateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "RAND": {
                this.sqlNode = RandFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LOG2": {
                this.sqlNode = Log2Function.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LOG": {
                this.sqlNode = LogFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "CONV": {
                this.sqlNode = ConvFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SCHEMA":
            case "DATABASE": {
                this.sqlNode = MycatDatabaseFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "SLEEP": {
                this.sqlNode = MycatSleepFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LAST_INSERT_ID": {
                this.sqlNode = MycatLastInsertIdFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "ROW_COUNT": {
                this.sqlNode = MycatRowCountFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "VERSION": {
                this.sqlNode = MycatVersionFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "CONNECTION_ID": {
                this.sqlNode = MycatConnectionIdFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "CURRENT_USER": {
                this.sqlNode = MycatCurrentUserFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "SESSION_USER":
            case "SYSTEM_USER":
            case "USER": {
                this.sqlNode = MycatUserFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                return false;
            }
            case "ADDDATE": {
                SQLExpr sqlExpr = x.getArguments().get(1);
                if (x.getArguments().size() > 1 &&
                        sqlExpr instanceof com.alibaba.druid.sql.ast.expr.SQLIntegerExpr) {
                    argNodes.set(1, convertToSqlNode(new SQLIntervalExpr(sqlExpr, SQLIntervalUnit.DAY)));
                }
            }
            case "DATE_ADD": {
                this.sqlNode = DateAddFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SUBDATE": {
                SQLExpr sqlExpr = x.getArguments().get(1);
                if (x.getArguments().size() > 1 &&
                        sqlExpr instanceof com.alibaba.druid.sql.ast.expr.SQLIntegerExpr) {
                    argNodes.set(1, convertToSqlNode(new SQLIntervalExpr(sqlExpr, SQLIntervalUnit.DAY)));
                }
            }
            case "DATE_SUB":
                this.sqlNode = DateSubFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            case "SUBTIME": {
                this.sqlNode = SubTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIME_FORMAT": {
                this.sqlNode = TimeFormatFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIME_TO_SEC": {
                this.sqlNode = TimeToSecFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TO_DAYS": {
                this.sqlNode = ToDaysFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TO_SECONDS": {
                this.sqlNode = ToSecondsFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "UTC_DATE": {
                this.sqlNode = UtcDateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "UTC_TIME": {
                this.sqlNode = UtcTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "UTC_TIMESTAMP": {
                this.sqlNode = UtcTimestampFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "WEEK": {
                this.sqlNode = WeekFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "WEEKDAY": {
                this.sqlNode = WeekDayFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "WEEKOFYEAR": {
                this.sqlNode = WeekOfYearFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "YEAR": {
                this.sqlNode = YearFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "YEARWEEK": {
                this.sqlNode = YearWeekFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SYSDATE": {
                this.sqlNode = SysDateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIME": {
                this.sqlNode = TimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIMEDIFF": {
                this.sqlNode = TimeDiff2Function.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIMESTAMP": {
                if (argNodes.size() == 1) {
                    this.sqlNode = Timestamp2Function.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                    return false;
                }
                if (argNodes.size() == 2) {
                    this.sqlNode = TimestampComposeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                    return false;
                }
                throw new UnsupportedOperationException();
            }
            case "DAY":
            case "DAYOFMONTH": {
                ImmutableList<SqlNode> sqlNodes = ImmutableList.of(SqlLiteral.createSymbol(TimeUnitRange.DAY, SqlParserPos.ZERO), argNodes.get(0));
                this.sqlNode = ExtractFunction.INSTANCE.createCall(SqlParserPos.ZERO, sqlNodes);
                return false;
            }
            case "DAYOFWEEK": {
                this.sqlNode = DayOfWeekFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "FROM_DAYS": {
                this.sqlNode = FromDaysFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "DAYOFYEAR": {
                this.sqlNode = DayOfYearFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "HOUR": {
                this.sqlNode = HourFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LAST_DAY": {
                this.sqlNode = LastDayFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "LOCALTIMESTAMP":
            case "LOCALTIME":
            case "CURRENT_TIMESTAMP":
            case "NOW": {
                if (argNodes == null || argNodes.isEmpty()) {
                    this.sqlNode = NowFunction.INSTANCE.createCall(SqlParserPos.ZERO);
                } else {
                    this.sqlNode = NowNoArgFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                }
                return false;
            }
            case "MAKEDATE": {
                this.sqlNode = MakeDateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MAKETIME": {
                this.sqlNode = MakeTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "UNIX_TIMESTAMP": {
                this.sqlNode = UnixTimestampFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MICROSECOND": {
                this.sqlNode = MicrosecondFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MINUTE": {
                this.sqlNode = MinuteFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "PERIOD_ADD": {
                this.sqlNode = PeriodAddFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "PERIOD_DIFF": {
                this.sqlNode = PeriodDiffFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "QUARTER": {
                this.sqlNode = QuarterFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SECOND": {
                this.sqlNode = SecondFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "SEC_TO_TIME": {
                this.sqlNode = SecToTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "STR_TO_DATE": {
                this.sqlNode = SecToDateFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MONTHNAME": {
                this.sqlNode = MonthNameFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "MONTH": {
                this.sqlNode = MonthFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIMESTAMPDIFF": {
                if (argNodes.size() > 0 && argNodes.get(0) instanceof SqlIdentifier) {
                    SqlIdentifier arg0 = (SqlIdentifier) argNodes.get(0);
                    argNodes.set(0, SqlLiteral.createCharString(arg0.toString().toUpperCase(), SqlParserPos.ZERO));
                }
                this.sqlNode = TimestampDiffFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TIMESTAMPADD": {
                SQLExpr sqlExpr = arguments.get(0);
                if (sqlExpr instanceof SQLIdentifierExpr) {
                    SqlIdentifier arg0 = (SqlIdentifier) argNodes.get(0);
                    argNodes.set(0, SqlLiteral.createCharString(arg0.toString().toUpperCase(), SqlParserPos.ZERO));
                }
                this.sqlNode = TimestampAddFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "CONVERT": {
                SQLExpr usingExpr = x.getUsing();
                String using;
                if (usingExpr != null) {
                    using = SQLUtils.normalize(usingExpr.toString());
                } else {
                    using = "utf8";
                }
                //     SELECT CONVERT('abc' USING utf8);
                if (argNodes.size() == 1) {
                    argNodes.add(SqlLiteral.createCharString(using, SqlParserPos.ZERO));
                    this.sqlNode = ConvertFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                    return false;
                }
                this.sqlNode = SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "DATE_FORMAT": {
                this.sqlNode = DateFormatFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "FROM_UNIXTIME": {
                if (argNodes.size() == 1) {
                    this.sqlNode = FromUnixTimeFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                    return false;
                }
                this.sqlNode = FromUnixTimeFormatFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "GET_FORMAT": {
                this.sqlNode = GetFormatFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                        ImmutableList.of(
                                SqlLiteral.createCharString(argNodes.get(0).toString(), SqlParserPos.ZERO),
                                argNodes.get(1)
                        ));
                return false;
            }
            case "MYCATSESSIONVALUE": {
                this.sqlNode = MycatSessionValueFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "CHAR": {
                this.sqlNode = CharFunction.INSTANCE.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "CHAR_LENGTH": {
                this.sqlNode = SqlStdOperatorTable.CHARACTER_LENGTH.createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "TRIM": {
                if ("both".equalsIgnoreCase(x.getTrimOption())) {
                    functionOperator = TrimBothFunction.INSTANCE;
                } else if ("TRAILING".equalsIgnoreCase(x.getTrimOption())) {
                    functionOperator = TrimTrailingFunction.INSTANCE;
                } else if ("LEADING".equalsIgnoreCase(x.getTrimOption())) {
                    functionOperator = TrimLeadingFunction.INSTANCE;
                } else {
                    functionOperator = TrimBothFunction.INSTANCE;
                }
                sqlNode = Objects.requireNonNull(functionOperator).createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            case "ISNULL": {
                functionOperator = IS_NULL;
                sqlNode = Objects.requireNonNull(functionOperator).createCall(SqlParserPos.ZERO, argNodes);
                return false;
            }
            default:
                if (functionOperator == null) {
                    functionOperator = new SqlUnresolvedFunction(
                            new SqlIdentifier(methodName, SqlParserPos.ZERO),
                            null,
                            null,
                            null,
                            null,
                            SqlFunctionCategory.USER_DEFINED_FUNCTION);
                }
                sqlNode = functionOperator.createCall(SqlParserPos.ZERO, argNodes);
                return false;
        }
//        return false;
    }

    public boolean visit(SQLInListExpr x) {
        SqlNodeList sqlNodes = convertToSqlNodeList(x.getTargetList());
        SqlOperator sqlOperator = x.isNot() ? SqlStdOperatorTable.NOT_IN : SqlStdOperatorTable.IN;
        sqlNode = new SqlBasicCall(sqlOperator, new SqlNode[]{convertToSqlNode(x.getExpr()), sqlNodes},
                SqlParserPos.ZERO);

        return false;
    }

    public boolean visit(SQLVariantRefExpr x) {
        if ("?".equals(x.getName())) {
            this.sqlNode = new SqlDynamicParam(x.getIndex(),
                    SqlParserPos.ZERO);
            return false;
        } else {
            this.sqlNode = handleSQLVariantRefExpr(x.getName(), x);
            return false;
        }
    }

    @Override
    public boolean visit(SQLUnaryExpr x) {
        SQLUnaryOperator operator = x.getOperator();
        switch (operator) {
            case Not:
            case NOT:
                this.sqlNode = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO,
                        convertToSqlNode(x.getExpr()));
                break;
            case Negative:
                this.sqlNode = SqlStdOperatorTable.UNARY_MINUS.createCall(SqlParserPos.ZERO,
                        convertToSqlNode(x.getExpr()));
                break;
            case BINARY:
                this.sqlNode = BinaryFunction.INSTANCE.createCall(SqlParserPos.ZERO,
                        convertToSqlNode(x.getExpr()));
                break;
            case Compl:

            default:
                throw new UnsupportedOperationException(operator.name());
        }
        return false;
    }

    protected SqlNodeList convertToSqlNodeList(SQLExpr expr) {
        if (expr instanceof SQLListExpr) {
            return convertToSqlNodeList(((SQLListExpr) expr).getItems());
        } else {
            List<SqlNode> nodes = new ArrayList<SqlNode>(1);
            return new SqlNodeList(nodes, SqlParserPos.ZERO);
        }
    }

    protected SqlNodeList convertToSqlNodeList(List<? extends SQLExpr> exprList) {
        final int size = exprList.size();

        List<SqlNode> nodes = new ArrayList<SqlNode>(size);
        for (int i = 0; i < size; ++i) {
            SQLExpr expr = exprList.get(i);
            SqlNode node;
            if (expr instanceof SQLListExpr) {
                node = convertToSqlNodeList(((SQLListExpr) expr).getItems());
            } else {
                node = convertToSqlNode(expr);
            }
            nodes.add(node);
        }

        return new SqlNodeList(nodes, SqlParserPos.ZERO);
    }


    protected SqlNode convertToSqlNode(SQLObject ast) {
        if (ast == null) {
            return null;
        }
        MycatCalciteMySqlNodeVisitor visitor;
        if (ast instanceof SQLSelect||ast instanceof SQLExprTableSource){
            visitor  = new MycatCalciteMySqlNodeVisitor(++qbIds);
        }else {
            visitor  = new MycatCalciteMySqlNodeVisitor(qbIds);
        }

        ast.accept(visitor);
        return visitor.getSqlNode();
    }

    private SqlNodeList convertOrderby(SQLOrderBy orderBy) {
        if (orderBy == null) {
            //org/apache/calcite/calcite-core/1.23.0/calcite-core-1.23.0-sources.jar!/org/apache/calcite/sql/validate/SqlValidatorImpl.java:1353
            return new SqlNodeList(Collections.emptyList(), SqlParserPos.ZERO);
        }

        List<SQLSelectOrderByItem> items = orderBy.getItems();
        List<SqlNode> orderByNodes = new ArrayList<SqlNode>(items.size());

        for (SQLSelectOrderByItem item : items) {
            SqlNode node = convertToSqlNode(item.getExpr());
            if (item.getType() == SQLOrderingSpecification.DESC) {
                node = new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[]{node}, SqlParserPos.ZERO);
            }
            SQLSelectOrderByItem.NullsOrderType nullsOrderType = item.getNullsOrderType();
            if (nullsOrderType != null) {
                switch (nullsOrderType) {
                    case NullsFirst:
                        node = new SqlBasicCall(SqlStdOperatorTable.NULLS_FIRST, new SqlNode[]{node}, SqlParserPos.ZERO);
                        break;
                    case NullsLast:
                        node = new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, new SqlNode[]{node}, SqlParserPos.ZERO);
                        break;
                    default:
                        break;
                }
            }
            orderByNodes.add(node);
        }

        return new SqlNodeList(orderByNodes, SqlParserPos.ZERO);
    }

    private SqlNodeList convertHints(List<SQLCommentHint> hints) {
        return convertHints(hints, null);
    }

    private SqlNodeList convertHints(List<SQLCommentHint> hints, String alias) {
        if (hints == null) {
            return new SqlNodeList(SqlParserPos.ZERO);
        }
        List<MycatHint> list = new ArrayList<>(hints.size() + 1);
        boolean QB_NAME = false;
        for (SQLCommentHint hint : hints) {
            list.add(new MycatHint(hint.getText()));
            QB_NAME = hint.getText().contains("QB_NAME");
        }
        if (!QB_NAME) {
            String format;
            if (alias == null) {
                format = "+MYCAT:" +
                        "QB_NAME(SEL$" + qbIds + ")" +
                        "";
            } else {
                format = "+MYCAT:" +
                        "QB_NAME(" +alias+
                        ")" +
                        "";
            }

            list.add(new MycatHint(format));
        }
        ImmutableList.Builder<SqlHint> listBuilder = ImmutableList.builder();
        for (MycatHint hint : list) {
            for (MycatHint.Function function : hint.getFunctions()) {
                String functionName = function.getName();
                boolean kv = !function.getArguments().isEmpty() && (function.getArguments().get(0) instanceof MycatHint.Argument)
                        && function.getArguments().get(0).getName() != null;
                ImmutableList.Builder<Object> builder = ImmutableList.builder();
                for (MycatHint.Argument argument : function.getArguments()) {
                    String argumentName = Optional.ofNullable(argument.getName()).map(i -> (argument.getName().toString())).orElse(null);
                    String value = SQLUtils.normalize(argument.getValue().toString());
                    if (!kv) {
                        builder.add(new SqlIdentifier(value, SqlParserPos.ZERO));
                    } else {
                        builder.add(
                                new SqlNodeList(Arrays.asList(new SqlIdentifier(argumentName, SqlParserPos.ZERO),
                                        new SqlIdentifier(value, SqlParserPos.ZERO)), SqlParserPos.ZERO)
                        );
                    }
                }
                SqlNodeList sqlNodes = new SqlNodeList((List) builder.build(), SqlParserPos.ZERO);
                SqlHint sqlHint =
                        new SqlHint(SqlParserPos.ZERO,
                                new SqlIdentifier(functionName, SqlParserPos.ZERO), sqlNodes, kv ? SqlHint.HintOptionFormat.KV_LIST : SqlHint.HintOptionFormat.ID_LIST);
                listBuilder.add(sqlHint);
            }
        }
        hint = true;
        return (new SqlNodeList(listBuilder.build(), SqlParserPos.ZERO));
    }

    /**
     * If there are multiple VALUES, and all values in VALUES CLAUSE are literal,
     * convert the value clauses to a single value clause.
     *
     * @param valuesClauseList
     * @return
     */
    public static List<SQLInsertStatement.ValuesClause> convertToSingleValuesIfNeed(List<SQLInsertStatement.ValuesClause> valuesClauseList) {
        if (valuesClauseList.size() <= 1) {
            return valuesClauseList;
        }

        // If they are all literals
        for (SQLInsertStatement.ValuesClause clause : valuesClauseList) {
            for (SQLExpr expr : clause.getValues()) {
                if (expr instanceof SQLVariantRefExpr) {
                    if (((SQLVariantRefExpr) expr).getName().equals("?")) {
                        continue;
                    }
                }
                return valuesClauseList;
            }
        }

        // Return only the first values clause.
        return Arrays.asList(valuesClauseList.get(0));
    }

    public boolean visit(SQLIntervalExpr x) {
        TimeUnit timeUnits[] = getTimeUnit(x.getUnit());
        SqlIntervalQualifier unitNode = new SqlIntervalQualifier(timeUnits[0], timeUnits[1], SqlParserPos.ZERO);
        SqlLiteral valueNode = (SqlLiteral) convertToSqlNode(x.getValue());
        sqlNode = SqlIntervalLiteral.createInterval(1, valueNode.toValue(), unitNode, SqlParserPos.ZERO);
        return false;
    }

    public static TimeUnit[] getTimeUnit(SQLIntervalUnit unit) {
        TimeUnit[] timeUnits = new TimeUnit[2];
        switch (unit) {
            case SECOND_MICROSECOND:
                timeUnits[0] = TimeUnit.SECOND;
                timeUnits[1] = TimeUnit.MICROSECOND;
                break;
            case MICROSECOND:
                timeUnits[0] = TimeUnit.MICROSECOND;
                timeUnits[1] = null;
                break;
            case SECOND:
                timeUnits[0] = TimeUnit.SECOND;
                timeUnits[1] = null;
                break;
            case MINUTE:
                timeUnits[0] = TimeUnit.MINUTE;
                timeUnits[1] = null;
                break;
            case HOUR:
                timeUnits[0] = TimeUnit.HOUR;
                timeUnits[1] = null;
                break;
            case DAY:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = null;
                break;
            case WEEK:
                timeUnits[0] = TimeUnit.WEEK;
                timeUnits[1] = null;
                break;
            case MONTH:
                timeUnits[0] = TimeUnit.MONTH;
                timeUnits[1] = null;
                break;
            case QUARTER:
                timeUnits[0] = TimeUnit.QUARTER;
                timeUnits[1] = null;
                break;
            case YEAR:
                timeUnits[0] = TimeUnit.YEAR;
                timeUnits[1] = null;
                break;
            case MINUTE_MICROSECOND:
                timeUnits[0] = TimeUnit.MINUTE;
                timeUnits[1] = TimeUnit.MICROSECOND;
                break;
            case MINUTE_SECOND:
                timeUnits[0] = TimeUnit.MINUTE;
                timeUnits[1] = TimeUnit.SECOND;
                break;
            case HOUR_MICROSECOND:
                timeUnits[0] = TimeUnit.HOUR;
                timeUnits[1] = TimeUnit.MICROSECOND;
                break;
            case HOUR_SECOND:
                timeUnits[0] = TimeUnit.HOUR;
                timeUnits[1] = TimeUnit.SECOND;
                break;
            case HOUR_MINUTE:
                timeUnits[0] = TimeUnit.HOUR;
                timeUnits[1] = TimeUnit.MINUTE;
                break;
            case DAY_MICROSECOND:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.MICROSECOND;
                break;
            case DAY_SECOND:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.SECOND;
                break;
            case DAY_MINUTE:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.MINUTE;
                break;
            case DAY_HOUR:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.HOUR;
                break;
            case YEAR_MONTH:
                timeUnits[0] = TimeUnit.YEAR;
                timeUnits[1] = TimeUnit.MONTH;
                break;
            case DAY_OF_WEEK:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.WEEK;
                break;
            case DOW:
                timeUnits[0] = TimeUnit.DOW;
                timeUnits[1] = null;
                break;
            case DAY_OF_MONTH:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.MONTH;
                break;
            case DAY_OF_YEAR:
                timeUnits[0] = TimeUnit.DAY;
                timeUnits[1] = TimeUnit.YEAR;
                break;
            case YEAR_OF_WEEK:
                timeUnits[0] = TimeUnit.YEAR;
                timeUnits[1] = TimeUnit.WEEK;
                break;
            case YOW:
                throw new ParserException("Unsupported time unit");
            case TIMEZONE_HOUR:
                timeUnits[0] = TimeUnit.HOUR;
                timeUnits[1] = TimeUnit.HOUR;
                break;
            case TIMEZONE_MINUTE:
                timeUnits[0] = TimeUnit.MINUTE;
                timeUnits[1] = TimeUnit.MINUTE;
                break;
            case DOY:
                timeUnits[0] = TimeUnit.DOY;
                timeUnits[1] = null;
                break;
            case YEAR_TO_MONTH:
                timeUnits[0] = TimeUnit.YEAR;
                timeUnits[1] = TimeUnit.MONTH;
                break;
            default:
                throw new ParserException("Unsupported time unit");
        }
        return timeUnits;
    }

    public boolean visit(SQLNotExpr x) {
        SQLExpr expr = x.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
            long hashCode64 = ((SQLIdentifierExpr) expr).nameHashCode64();
            if (hashCode64 == FnvHash.Constants.UNKNOWN) {
                sqlNode = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, SqlLiteral.createUnknown(SqlParserPos.ZERO));
                return false;
            }
        }
        expr.accept(this);
        sqlNode = SqlStdOperatorTable.NOT.createCall(SqlParserPos.ZERO, sqlNode);
        return false;
    }

    @Override
    public boolean visit(SQLExtractExpr x) {
        SqlNode sqlNode = convertToSqlNode(x.getValue());
        TimeUnit timeUnits[] = getTimeUnit(x.getUnit());
        TimeUnitRange range = TimeUnitRange.of(timeUnits[0], timeUnits[1]);
        ImmutableList<SqlNode> sqlNodes = ImmutableList.of(SqlLiteral.createSymbol(range, SqlParserPos.ZERO), sqlNode);

        this.sqlNode = ExtractFunction.INSTANCE
                .createCall(SqlParserPos.ZERO, sqlNodes);
        return false;
    }

    @Override
    public boolean visit(SQLGroupingSetExpr x) {
        sqlNode = SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO
                , convertToSqlNodeList(x.getParameters())
        );
        return false;
    }

    @Override
    public boolean visit(SQLValuesQuery x) {
        List<SqlNode> valuesNodes = new ArrayList<SqlNode>();
        for (SQLExpr value : x.getValues()) {
            valuesNodes.add(
                    SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, convertToSqlNodeList(value)));
        }

        sqlNode = SqlStdOperatorTable.VALUES.createCall(SqlParserPos.ZERO, valuesNodes);
        return false;
    }


    @Override
    public boolean visit(SQLUnnestTableSource x) {
        sqlNode = SqlStdOperatorTable.UNNEST
                .createCall(SqlParserPos.ZERO
                        , convertToSqlNodeList(x.getItems()));

        String alias = x.getAlias();
        if (alias != null) {
            sqlNode = new SqlBasicCall(SqlStdOperatorTable.AS
                    , new SqlNode[]{sqlNode, new SqlIdentifier(SQLUtils.normalize(alias), SqlParserPos.ZERO)}
                    , SqlParserPos.ZERO
            );
        }
        return false;
    }

    @Override
    public boolean visit(SQLDefaultExpr x) {
        sqlNode = SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(SQLHexExpr x) {
        sqlNode = SqlLiteral.createBinaryString(x.getHex(), SqlParserPos.ZERO);
        return false;
    }

    @Override
    public boolean visit(MySqlExplainStatement x) {
        x.getStatement().accept(this);
        SqlNode explicandum = this.sqlNode;
        sqlNode = new SqlExplain(SqlParserPos.ZERO
                , explicandum
                , SqlLiteral.createSymbol(SqlExplainLevel.EXPPLAN_ATTRIBUTES, SqlParserPos.ZERO)
                , SqlLiteral.createSymbol(SqlExplain.Depth.PHYSICAL, SqlParserPos.ZERO)
                , SqlLiteral.createSymbol(SqlExplainFormat.TEXT, SqlParserPos.ZERO)
                , 0
        );
        return false;
    }

    @Override
    public boolean visit(SQLDataType x) {
        ;
        SqlTypeName sqlTypeName = SqlTypeName.valueOf(x.getName().toUpperCase());
        SqlBasicTypeNameSpec sqlBasicTypeNameSpec = new SqlBasicTypeNameSpec(sqlTypeName, -1, -1, null, SqlParserPos.ZERO);
        sqlNode = new SqlDataTypeSpec(sqlBasicTypeNameSpec, SqlParserPos.ZERO);
        return false;
    }

    static long JSON_VALUE = FnvHash.fnv1a_64_lower("JSON VALUE");
    static long JSON_OBJECT = FnvHash.fnv1a_64_lower("JSON OBJECT");
    static long JSON_ARRAY = FnvHash.fnv1a_64_lower("JSON ARRAY");
    static long JSON_SCALAR = FnvHash.fnv1a_64_lower("JSON SCALAR");

    public boolean isHint() {
        return hint;
    }
}

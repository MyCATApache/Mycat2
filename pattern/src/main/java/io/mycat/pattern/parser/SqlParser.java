package io.mycat.pattern.parser;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastsql.sql.parser.ParserException;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import io.mycat.pattern.parser.SchemaItem.ColumnItem;
import io.mycat.pattern.parser.SchemaItem.FieldItem;
import io.mycat.pattern.parser.SchemaItem.RelationFieldsPair;
import io.mycat.pattern.parser.SchemaItem.TableItem;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlParser {

    public static void main(String[] args) {
        SchemaItem schemaItem =
                SqlParser.parse("select t1.id,t1.name,t1.pwd from user t1 " +
                        "left join order t2 on t2.user_id = t1.id " +
                        "where t1.name = '小王' and t2.id = '' and t1.id = 1 and t1.de_flag = 0;");
        System.out.println(schemaItem);
    }

    /**
     * 解析sql
     *
     * @param sql sql
     * @return 视图对象
     */
    public static SchemaItem parse(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

        SchemaItem schemaItem = new SchemaItem();
        schemaItem.setSql(SQLUtils.toMySqlString(sqlSelectQueryBlock));
        SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
        List<TableItem> tableItems = new ArrayList<>();
        SqlParser.visitSelectTable(schemaItem, sqlTableSource, tableItems, null);
        tableItems.forEach(tableItem -> schemaItem.getAliasTableItems().put(tableItem.getAlias(), tableItem));

        List<FieldItem> selectFieldItems = collectSelectQueryFields(sqlSelectQueryBlock);
        selectFieldItems.forEach(fieldItem -> {
            schemaItem.getSelectFields().put(fieldItem.getFieldName(), fieldItem);
            schemaItem.getTotalFields().put(fieldItem.getOwnerAndColumnName(), fieldItem);
        });

        List<FieldItem> whereFieldItems = new ArrayList<>();
        collectWhereFields(sqlSelectQueryBlock.getWhere(),null,null,whereFieldItems);
        whereFieldItems.forEach(fieldItem -> {
            schemaItem.getTotalFields().put(fieldItem.getOwnerAndColumnName(), fieldItem);
        });

        schemaItem.init();
        schemaItem.setWhereFieldItems(whereFieldItems);

//            if (schemaItem.getAliasTableItems().isEmpty() || schemaItem.getSelectFields().isEmpty()) {
//                throw new ParserException("Parse sql error");
//            }
        return schemaItem;
    }


    /**
     * 归集字段 (where 条件中的)
     * @param left
     * @param right
     * @param fieldItems
     */
    private static void collectWhereFields(SQLExpr left, SQLExpr right,SQLBinaryOpExpr expr, List<FieldItem> fieldItems) {
        if(left instanceof SQLBinaryOpExpr){
            collectWhereFields(((SQLBinaryOpExpr) left).getLeft(),((SQLBinaryOpExpr) left).getRight(), (SQLBinaryOpExpr) left,fieldItems);
        }
        if(right instanceof SQLBinaryOpExpr){
            collectWhereFields(((SQLBinaryOpExpr) right).getLeft(),((SQLBinaryOpExpr) right).getRight(), (SQLBinaryOpExpr) right,fieldItems);
        }
        FieldItem leftFieldItem = new FieldItem();
        FieldItem rightFieldItem = new FieldItem();
        if(expr != null && expr.getOperator() != null){
            leftFieldItem.setOperator(expr.getOperator().getName());
            rightFieldItem.setOperator(expr.getOperator().getName());
        }
        visitColumn(left,leftFieldItem);
        visitColumn(right,rightFieldItem);
        if(left instanceof SQLValuableExpr){
            rightFieldItem.setExistValue(Boolean.TRUE);
            rightFieldItem.setValue(((SQLValuableExpr) left).getValue());
            fieldItems.add(rightFieldItem);
        }
        if(right instanceof SQLValuableExpr){
            leftFieldItem.setExistValue(Boolean.TRUE);
            leftFieldItem.setValue(((SQLValuableExpr) right).getValue());
            fieldItems.add(leftFieldItem);
        }
    }

    /**
     * 归集字段
     *
     * @param sqlSelectQueryBlock sqlSelectQueryBlock
     * @return 字段属性列表
     */
    private static List<FieldItem> collectSelectQueryFields(MySqlSelectQueryBlock sqlSelectQueryBlock) {
        return sqlSelectQueryBlock.getSelectList().stream().map(selectItem -> {
            FieldItem fieldItem = new FieldItem();
            fieldItem.setFieldName(selectItem.getAlias());
            fieldItem.setExpr(selectItem.toString());
            visitColumn(selectItem.getExpr(), fieldItem);
            return fieldItem;
        }).collect(Collectors.toList());
    }

    /**
     * 解析字段
     *
     * @param expr sql expr
     * @param fieldItem 字段属性
     */
    private static void visitColumn(SQLExpr expr, FieldItem fieldItem) {
        if(expr == null){
            return;
        }
        if (expr instanceof SQLIdentifierExpr) {
            // 无owner
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(identifierExpr.getName());
                fieldItem.setExpr(identifierExpr.toString());
            }
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(identifierExpr.getName());
            fieldItem.getOwners().add(null);
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLPropertyExpr) {
            // 有owner
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
            if (fieldItem.getFieldName() == null) {
                fieldItem.setFieldName(sqlPropertyExpr.getName());
                fieldItem.setExpr(sqlPropertyExpr.toString());
            }
            fieldItem.getOwners().add(sqlPropertyExpr.getOwnernName());
            ColumnItem columnItem = new ColumnItem();
            columnItem.setColumnName(sqlPropertyExpr.getName());
            columnItem.setOwner(sqlPropertyExpr.getOwnernName());
            fieldItem.addColumn(columnItem);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            fieldItem.setMethod(true);
            for (SQLExpr sqlExpr : methodInvokeExpr.getArguments()) {
                visitColumn(sqlExpr, fieldItem);
            }
        } else if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            fieldItem.setBinaryOp(true);
            visitColumn(sqlBinaryOpExpr.getLeft(), fieldItem);
            visitColumn(sqlBinaryOpExpr.getRight(), fieldItem);
        } else if (expr instanceof SQLCaseExpr) {
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) expr;
            fieldItem.setMethod(true);
            sqlCaseExpr.getItems().forEach(item-> visitColumn(item.getConditionExpr(), fieldItem));
        }else if(expr instanceof SQLValuableExpr) {
            //skip
        }else {
            throw new UnsupportedOperationException("no support Column. expr="+ expr);
//            LOGGER.warn("skip filed. expr={}",expr);
        }
    }

    /**
     * 解析表
     *
     * @param schemaItem 视图对象
     * @param sqlTableSource sqlTableSource
     * @param tableItems 表对象列表
     * @param tableItemTmp 表对象(临时)
     */
    private static void visitSelectTable(SchemaItem schemaItem, SQLTableSource sqlTableSource,
                                         List<TableItem> tableItems, TableItem tableItemTmp) {
        if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setSchema(sqlExprTableSource.getSchema());
            tableItem.setTableName(sqlExprTableSource.getTableName());
            if (tableItem.getAlias() == null) {
                tableItem.setAlias(sqlExprTableSource.getAlias());
            }
            if (tableItems.isEmpty()) {
                // 第一张表为主表
                tableItem.setMain(true);
            }
            tableItems.add(tableItem);
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource leftTableSource = sqlJoinTableSource.getLeft();
            visitSelectTable(schemaItem, leftTableSource, tableItems, null);
            SQLTableSource rightTableSource = sqlJoinTableSource.getRight();
            TableItem rightTableItem = new TableItem(schemaItem);
            // 解析on条件字段
            visitOnCondition(sqlJoinTableSource.getCondition(), rightTableItem);
            visitSelectTable(schemaItem, rightTableSource, tableItems, rightTableItem);

        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            MySqlSelectQueryBlock sqlSelectQuery = (MySqlSelectQueryBlock) subQueryTableSource.getSelect().getQuery();
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem(schemaItem);
            }
            tableItem.setAlias(subQueryTableSource.getAlias());
            tableItem.setSubQuerySql(SQLUtils.toMySqlString(sqlSelectQuery));
            tableItem.setSubQuery(true);
            tableItem.setSubQueryFields(collectSelectQueryFields(sqlSelectQuery));
            visitSelectTable(schemaItem, sqlSelectQuery.getFrom(), tableItems, tableItem);
        }else {
            throw new UnsupportedOperationException("no support SelectTable. sqlTable="+ sqlTableSource);
        }
    }

    /**
     * 解析on条件
     *
     * @param expr sql expr
     * @param tableItem 表对象
     */
    private static void visitOnCondition(SQLExpr expr, TableItem tableItem) {
        if ((expr instanceof SQLBinaryOpExpr)) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLBinaryOperator operator = sqlBinaryOpExpr.getOperator();
            FieldItem leftFieldItem = new FieldItem();
            leftFieldItem.setOperator(operator.getName());
            visitColumn(sqlBinaryOpExpr.getLeft(), leftFieldItem);

            FieldItem rightFieldItem = new FieldItem();
            rightFieldItem.setOperator(operator.getName());
            visitColumn(sqlBinaryOpExpr.getRight(), rightFieldItem);

            /*
             * 增加属性 -> 表用到的所有字段
             * 2019年6月6日 13:37:41 王子豪
             */
            tableItem.getSchemaItem().getTotalFields().put(leftFieldItem.getOwnerAndColumnName(),leftFieldItem);
            tableItem.getSchemaItem().getTotalFields().put(rightFieldItem.getOwnerAndColumnName(),rightFieldItem);

            tableItem.getRelationFields().add(new RelationFieldsPair(leftFieldItem, rightFieldItem));
        }else {
            throw new UnsupportedOperationException("no support SelectTable. OnCondition="+ expr);
        }
    }
}

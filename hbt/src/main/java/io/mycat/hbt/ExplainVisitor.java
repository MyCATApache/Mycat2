/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.hbt.ast.query.*;
import io.mycat.hbt.parser.HBTParser;
import org.apache.calcite.avatica.util.ByteString;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author jamie12221
 **/
public class ExplainVisitor implements NodeVisitor {
    final StringBuilder sb = new StringBuilder();
    int tagCount = 0;
    boolean dot = true;
    boolean forceField = false;

    final List<String> comments = new ArrayList<>(1);

    void enter() {
        tagCount++;

    }

    void leave() {
        --tagCount;
    }

    void append(String text) {
        sb.append(text);
    }

    public ExplainVisitor() {
        this(false);
    }

    public ExplainVisitor(boolean forceField) {
        this.forceField = forceField;
    }

    @Override
    public void visit(MapSchema mapSchema) {
        Schema schema = mapSchema.getSchema();
        List<Expr> expr = mapSchema.getExpr();

        String funName = "map";
        writeSchema(schema, funName);
        joinNode(expr);
        append(")\n");
        leave();

    }

    private void writeSchema(Schema schema, String funName) {
        if (!dot) {
            append(funName);
            append("(");
            schema.accept(this);
            append(",");
        } else {
            schema.accept(this);
            append(".");
            append(funName);
            append("(");
        }
        enter();
    }


    @Override
    public void visit(GroupBySchema groupSchema) {
        List<AggregateCall> exprs = groupSchema.getExprs();
        List<GroupKey> keys = groupSchema.getKeys();
        Schema schema = groupSchema.getSchema();
        writeSchema(schema, "groupBy");
        append("keys(");
        groupKey(keys);
        append(")");
        append(",");
        append("aggregating(");
        joinNode(exprs);
        append(")");
        append(")");
        leave();

    }


    private void groupKey(List<GroupKey> keys) {
        int size = keys.size();
        int lastIndex = keys.size() - 1;
        for (int i = 0; i < size; i++) {
            GroupKey key = keys.get(i);
            append("groupKey(");
            joinNode(key.getExprs());
            append(")");
            if (i != lastIndex) {
                append(",");
            }
        }
    }

    @Override
    public void visit(LimitSchema limitSchema) {
        writeSchema(limitSchema.getSchema(), "limit");
        Number offset = limitSchema.getOffset();
        append(Objects.toString(offset));
        append(",");
        Number limit = limitSchema.getLimit();
        append(Objects.toString(limit));
        append(")");
        leave();
    }

    @Override
    public void visit(FromTableSchema fromSchema) {
        append("fromTable(");
        append(fromSchema.getNames().stream().map(i -> toId(i)).collect(Collectors.joining(",")));
        append(")");
    }

    @Override
    public void visit(SetOpSchema setOpSchema) {
        HBTOp op = setOpSchema.getOp();
        append(op.getFun());
        enter();
        append("(");
        List<Schema> schemas = setOpSchema.getSchemas();
        int size = schemas.size();
        for (int i = 0; i < size - 1; i++) {
            Schema schema = schemas.get(i);
            append("\n ");
            schema.accept(this);
            append(",");
        }
        append("\n ");
        schemas.get(size - 1).accept(this);
        append("\n");
        append(")");
        leave();
    }

    @Override
    public void visit(FieldType fieldSchema) {
        String id = fieldSchema.getColumnName();
        String type = fieldSchema.getColumnType();
        boolean nullable = fieldSchema.isNullable();
        Integer precision = fieldSchema.getPrecision();
        Integer scale = fieldSchema.getScale();
        if (precision != null && precision > 0) {
            if (scale != null) {
                append(MessageFormat.format("fieldType({0},{1},{2},{3},{4})", toId(id), toId(type), toId(Boolean.toString(nullable)),
                        toId(Integer.toString(precision)), toId(Integer.toString(scale))));
            }else {
                append(MessageFormat.format("fieldType({0},{1},{2},{3})", toId(id), toId(type), toId(Boolean.toString(nullable)),
                        toId(Integer.toString(precision))));
            }
        } else {
            append(MessageFormat.format("fieldType({0},{1},{2})", toId(id), toId(type), toId(Boolean.toString(nullable))));
        }
    }

    @Override
    public void visit(Literal literal) {
        Object value = literal.getValue();
        String target;
        if (value instanceof String) {
            target = "'" + value + "'";
        } else if (value instanceof byte[]) {
            byte[] value1 = (byte[]) value;
            ByteString byteString = new ByteString(value1);
            target = "X'" + byteString.toString() + "'";
        } else if (value instanceof Number) {
            target = "" + value + "";
        } else if (value instanceof LocalDate) {
            target = "" + (value) + "";
        } else if (value instanceof LocalDateTime) {
            target = "" + (value) + "";
        } else if (value instanceof LocalTime) {
            target = "" + (value) + "";
        } else {
            target = "" + value + "";
        }
        append(target);
    }

    @Override
    public void visit(OrderSchema orderSchema) {
        List<OrderItem> orders = orderSchema.getOrders();
        if (orders.isEmpty()) {
            orderSchema.getSchema().accept(this);
        } else {
            writeSchema(orderSchema.getSchema(), "orderBy");
            String orderListText = getOrderListString(orders);
            append(orderListText);
            append(")");
            leave();
        }

    }

    private String getOrderListString(List<OrderItem> orders) {
        return orders.stream().map(i -> {
            String columnName = i.getColumnName();
            String name = i.getDirection().name();
            return "order(" + toId(columnName) + "," + toId(name) + ")";
        }).collect(Collectors.joining(","));
    }

    @Override
    public void visit(Identifier identifier) {
        String value = identifier.getValue();
        append("`");
        append(value);
        append("`");
    }

    @Override
    public void visit(Expr expr) {
        String functionName = null;
        if (expr instanceof Fun) {
            Fun fun = (Fun) expr;
            functionName = (fun.getFunctionName());
        } else if (expr.op == HBTOp.AS_COLUMN_NAME) {
            functionName = ("as");
        } else if (expr.op == HBTOp.CAST) {
            functionName = ("cast");
        } else if (expr.op == HBTOp.DOT) {
            functionName = ("dot");
        } else if (expr.op == HBTOp.REF) {
            functionName = ("ref");
        } else {
            throw new UnsupportedOperationException();
        }
        Map<String, HBTParser.Precedence> operators = HBTCalciteSupport.INSTANCE.getOperators();
        if (expr.getNodes().size() == 2 && operators.containsKey(functionName)) {
            HBTParser.Precedence precedence = operators.get(functionName);
            expr.getNodes().get(0).accept(this);
            append(" ");
            append(functionName);
            append(" ");
            expr.getNodes().get(1).accept(this);
        } else {
            append(functionName);
            append("(");
            joinNode(expr.getNodes());
            append(")");
        }
    }


    @Override
    public void visit(AnonyTableSchema valuesSchema) {
        append("table(");
        append("fields(");
        joinNode(valuesSchema.getFieldNames());
        append(")");
        append(",");
        append("values(");
        joinNode(valuesSchema.getValues().stream().map(i -> new Literal(i)).collect(Collectors.toList()));
        append(")");
        append(")");
    }

    private void joinNode(List fieldNames) {
        if (fieldNames.isEmpty()) {
            return;
        }
        int size = fieldNames.size();
        for (int i = 0; i < size - 1; i++) {
            Node o = (Node) fieldNames.get(i);
            o.accept(this);
            append(",");
        }
        Node o = (Node) fieldNames.get(size - 1);
        o.accept(this);
    }

    @Override
    public void visit(JoinSchema corJoinSchema) {
        Expr condition = corJoinSchema.getCondition();
        append(corJoinSchema.getOp().getFun());
        append("(");
        append(getExprString(condition));


        append(",");
        append(getExprString(corJoinSchema.getLeft()));
        append(",");
        append(getExprString(corJoinSchema.getRight()));
        append(")");
    }

    private String getExprString(List condition) {
        return (String) condition.stream().map(i -> getExprString((Node) i)).collect(Collectors.joining(","));
    }

    private String getExprString(Node condition) {
        ExplainVisitor explainVisitor = new ExplainVisitor(false);
        condition.accept(explainVisitor);
        return explainVisitor.getString();
    }

    private String getExprStringWithField(Node condition) {
        ExplainVisitor explainVisitor = new ExplainVisitor(true);
        condition.accept(explainVisitor);
        return explainVisitor.getString();
    }

    @Override
    public void visit(AggregateCall aggregateCall) {
        final String function = aggregateCall.getFunction();
        final String alias = aggregateCall.getAlias(); // may be null
        final List<Expr> operands = aggregateCall.getOperands(); // may be empty, never null
        final Boolean distinct = aggregateCall.getDistinct();
        final Boolean approximate = aggregateCall.getApproximate();
        final Boolean ignoreNulls = aggregateCall.getIgnoreNulls();
        final Expr filter = aggregateCall.getFilter(); // may be null
        final List<OrderItem> orderKeys = aggregateCall.getOrderKeys(); // may be empty, never null
        String res = function + "(" + operands.stream().map(i -> getExprString(i)).collect(Collectors.joining(",")) + ")";
        append(res);
        res = "";
        if (alias != null) {
            res += ".alias(" + alias + ")";
        }
        if (Boolean.TRUE.equals(distinct)) {
            res += ".distinct(" + ")";
        }
        if (Boolean.TRUE.equals(approximate)) {
            res += ".approximate(" + ")";
        }
        if (Boolean.TRUE.equals(ignoreNulls)) {
            res += ".ignoreNulls(" + ")";
        }
        if (filter != null) {
            res += ".filter(" + getExprString(filter) + ")";
        }
        if (orderKeys != null && !orderKeys.isEmpty()) {
            res += ".orderBy(" + getOrderListString(orderKeys) + ")";
        }

    }

    @Override
    public void visit(FilterSchema filterSchema) {
        writeSchema(filterSchema.getSchema(), "filter");
        filterSchema.getExprs().accept(this);
        append(")");
        leave();
    }

    @Override
    public void visit(ModifyFromSql modifyTable) {
        String targetName = modifyTable.getTargetName();
        String sql = modifyTable.getSql().replaceAll("\n", " ");
        append(modifyTable.getOp().getFun());
        append("(");
        append(targetName);
        append(",");
        append("'");
        append(sql);
        append("'");
        append(")");
    }

    @Override
    public void visit(DistinctSchema distinctSchema) {
        writeSchema(distinctSchema.getSchema(), "distinct");
        append(")");
        leave();
    }

    @Override
    public void visit(RenameSchema projectSchema) {
        List<String> columnNames = projectSchema.getAlias();
        writeSchema(projectSchema.getSchema(), projectSchema.getOp().getFun());
        append(columnNames.stream().map(this::toId).collect(Collectors.joining(",")));
        append(")");
        leave();
    }

    private String toId(String i) {
        return i;
    }

    @Override
    public void visit(CorrelateSchema correlate) {
        append(correlate.getOp().getFun());
        append("(");
        append(correlate.getRefName());
        append(",");
        correlate.getLeft().accept(this);
        append(",");
        correlate.getRight().accept(this);
        append(")");
    }

    @Override
    public void visit(FromSqlSchema fromSqlSchema) {
        String targetName = fromSqlSchema.getTargetName();
        String sql = fromSqlSchema.getSql();
        append(fromSqlSchema.getOp().getFun());
        append("(");
        append(targetName);
        append(",");
        StringBuilder comment = new StringBuilder();
//        comment.append("targetName:").append(targetName).append("\n");

            comment.append("fields(");
            comment.append(getExprString(fromSqlSchema.getFieldTypes()));
            comment.append(")\n");
        append(comment.toString());
            append(",");
        append("\n");
        append("'");
        append(sql);
        append("'");
        append(")");

    }

    @Override
    public void visit(FilterFromTableSchema filterFromTableSchema) {
        String exprString = getExprString(filterFromTableSchema.getFilter());
        append(filterFromTableSchema.getOp().getFun());
        append("(");
        append(exprString);
        append(",");
        List<String> names = filterFromTableSchema.getNames();
        append(names.get(0));
        append(",");
        append(names.get(1));
        append(")");
    }

    @Override
    public void visit(FromRelToSqlSchema fromRelSchema) {
        String targetName = fromRelSchema.getTargetName();
        append(fromRelSchema.getOp().getFun());
        append("(");
        append(targetName);
        append(",");
        fromRelSchema.getRel().accept(this);
        append(")");
    }

    @Override
    public void visit(MergeModify mergeModify) {
        append(mergeModify.getOp().getFun());
        append("(");
        List<ModifyFromSql> list = StreamSupport.stream(mergeModify.getList().spliterator(), false).collect(Collectors.toList());
        int size = list.size();
        for (int i = 0; i < size - 1; i++) {
            ModifyFromSql modifyFromSql = list.get(i);
            modifyFromSql.accept(this);
            append(",");
        }
        list.get(size - 1).accept(this);
        append(")");
    }

    @Override
    public void visit(Param param) {
        append("?");
    }

    @Override
    public void visit(CommandSchema commandSchema) {
        String fun = commandSchema.getOp().getFun();
        append(fun);
        append("(");
        commandSchema.getSchema().accept(this);
        append(")");
    }


    public String getString() {
        return sb.toString();
    }
}
/**
 * Copyright (C) <2020>  <chen junwen>
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

import io.mycat.hbt.ast.AggregateCall;
import io.mycat.hbt.ast.Direction;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.modify.ModifyTable;
import io.mycat.hbt.ast.query.*;
import org.apache.calcite.avatica.util.ByteString;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jamie12221
 **/
public class ExplainVisitor implements NodeVisitor {
    final StringBuilder sb = new StringBuilder();

    public ExplainVisitor() {
    }

    @Override
    public void visit(MapSchema mapSchema) {
        Schema schema = mapSchema.getSchema();
        List<Expr> expr = mapSchema.getExpr();
        sb.append("map");
        sb.append("(");
        schema.accept(this);
        sb.append(",");
        joinNode(expr);
        sb.append(")");

    }


    @Override
    public void visit(GroupSchema groupSchema) {
        List<AggregateCall> exprs = groupSchema.getExprs();
        List<GroupItem> keys = groupSchema.getKeys();
        sb.append("groupBy(");
        Schema schema = groupSchema.getSchema();
        schema.accept(this);
        sb.append(",");
        sb.append("keys(");
        groupKey(keys);
        sb.append(")");
        sb.append(",");
        sb.append("aggregating(");
        joinNode(exprs);
        sb.append(")");
        sb.append(")");
    }

    private String orderKeys(List<OrderItem> orderKeys) {
        int size = orderKeys.size();
        int lastIndex = orderKeys.size() - 1;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append("order(");
            OrderItem orderItem = orderKeys.get(i);
            String columnName = orderItem.getColumnName();
            sb.append(columnName);
            Direction direction = orderItem.getDirection();
            sb.append(",");
            sb.append(direction.getName());
            sb.append(")");
            if (i != lastIndex) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private void groupKey(List<GroupItem> keys) {
        int size = keys.size();
        int lastIndex = keys.size() - 1;
        for (int i = 0; i < size; i++) {
            GroupItem key = keys.get(i);
            sb.append("groupKey(");
            joinNode(key.getExprs());
            sb.append(")");
            if (i != lastIndex) {
                sb.append(",");
            }
        }
    }

    @Override
    public void visit(LimitSchema limitSchema) {
        sb.append("limit(");
        limitSchema.getSchema().accept(this);
        sb.append(",");
        Number offset = limitSchema.getOffset();
        sb.append(offset);
        sb.append(",");
        Number limit = limitSchema.getLimit();
        sb.append(limit);
        sb.append(")");
    }

    @Override
    public void visit(FromTableSchema fromSchema) {
        sb.append("fromTable(");
        sb.append(fromSchema.getNames().stream().map(i -> toId(i)).collect(Collectors.joining(",")));
        sb.append(")");
    }

    @Override
    public void visit(SetOpSchema setOpSchema) {
        Op op = setOpSchema.getOp();
        sb.append(op.getFun()).append("(");
        joinNode(setOpSchema.getSchemas());
        sb.append(")");
    }

    @Override
    public void visit(FieldType fieldSchema) {
        String id = fieldSchema.getId();
        String type = fieldSchema.getType();
        sb.append(MessageFormat.format("fieldType({0},{1})", toId(id), toId(type)));
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
        sb.append(target);
    }

    @Override
    public void visit(OrderSchema orderSchema) {
        List<OrderItem> orders = orderSchema.getOrders();
        if (orders.isEmpty()) {
            orderSchema.getSchema().accept(this);
        } else {
            sb.append("orderBy(");
            orderSchema.getSchema().accept(this);
            sb.append(",");
            String orderListText = getOrderListString(orders);
            sb.append(orderListText);
            sb.append(")");
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
        sb.append("`").append(value).append("`");
    }

    @Override
    public void visit(Expr expr) {
        if (expr instanceof Fun) {
            Fun fun = (Fun) expr;
            sb.append(fun.getFunctionName());
        } else if (expr.op == Op.AS_COLUMNNAME) {
            sb.append("as");
        } else if (expr.op == Op.CAST) {
            sb.append("cast");
        } else if (expr.op == Op.DOT) {
            sb.append("dot");
        } else if (expr.op == Op.REF) {
            sb.append("ref");
        }
        sb.append("(");
        joinNode(expr.getNodes());
        sb.append(")");
    }


    @Override
    public void visit(ValuesSchema valuesSchema) {
        sb.append("table(");
        sb.append("fields(");
        joinNode(valuesSchema.getFieldNames());
        sb.append(")");
        sb.append(",");
        sb.append("values(");
        joinNode(valuesSchema.getValues().stream().map(i -> new Literal(i)).collect(Collectors.toList()));
        sb.append(")");
        sb.append(")");
    }

    private void joinNode(List fieldNames) {
        if (fieldNames.isEmpty()) {
            return;
        }
        int size = fieldNames.size();
        for (int i = 0; i < size - 1; i++) {
            Node o = (Node) fieldNames.get(i);
            o.accept(this);
            sb.append(",");
        }
        Node o = (Node) fieldNames.get(size - 1);
        o.accept(this);
    }

    @Override
    public void visit(JoinSchema corJoinSchema) {
        Expr condition = corJoinSchema.getCondition();
        sb.append(corJoinSchema.getOp().getFun()).append("(");
        sb.append(getExprString(condition));


        sb.append(",");
        corJoinSchema.getLeft().accept(this);
        sb.append(",");
        corJoinSchema.getRight().accept(this);
        sb.append(")");
    }

    private String getExprString(Expr condition) {
        ExplainVisitor explainVisitor = new ExplainVisitor();
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

        if (alias != null) {
            res += ".alias(" + alias + ")";
        }
        if (distinct != null) {
            res += ".distinct(" + ")";
        }
        if (approximate != null) {
            res += ".approximate(" + ")";
        }
        if (ignoreNulls != null) {
            res += ".ignoreNulls(" + ")";
        }
        if (filter != null) {
            res += ".filter(" + getExprString(filter) + ")";
        }
        if (orderKeys != null && !orderKeys.isEmpty()) {
            res += ".orderBy(" + getOrderListString(orderKeys) + ")";
        }
        sb.append(res);
    }

    @Override
    public void visit(FilterSchema filterSchema) {
        sb.append("filter(");
        filterSchema.getSchema().accept(this);
        sb.append(",");
        filterSchema.getExprs().accept(this);
        sb.append(")");
    }

    @Override
    public void visit(ModifyTable modifyTable) {

    }

    @Override
    public void visit(DistinctSchema distinctSchema) {
        sb.append("distinct(");
        distinctSchema.getSchema().accept(this);
        sb.append(")");
    }

    @Override
    public void visit(RenameSchema projectSchema) {
        List<String> columnNames = projectSchema.getColumnNames();
        sb.append("rename(");
        projectSchema.getSchema().accept(this);
        sb.append(",");
        sb.append(columnNames.stream().map(this::toId).collect(Collectors.joining(",")));
        sb.append(")");
    }

    private String toId(String i) {
        return i;
    }

    @Override
    public void visit(CorrelateSchema correlate) {
        sb.append(correlate.getOp().getFun())
                .append("(")
                .append(correlate.getRefName())
                .append(",");
        correlate.getLeft().accept(this);
        sb.append(",");
        correlate.getRight().accept(this);
        sb.append(")");
    }

    @Override
    public void visit(FromSqlSchema fromSqlSchema) {
        String targetName = fromSqlSchema.getTargetName();
        String sql = fromSqlSchema.getSql();
        sb.append(fromSqlSchema.getOp().getFun())
                .append("(")
                .append(targetName)
                .append(",");
        if (!fromSqlSchema.getFieldTypes().isEmpty()) {
            sb.append("fields(");
            joinNode(fromSqlSchema.getFieldTypes());
            sb.append(")");
            sb.append(",");
        }
        sb.append("'");
        sb.append(sql);
        sb.append("'");
        sb.append(")");

    }

    @Override
    public void visit(FilterFromTableSchema filterFromTableSchema) {
        String exprString = getExprString(filterFromTableSchema.getFilter());
        sb.append(filterFromTableSchema.getOp().getFun())
                .append("(")
                .append(exprString)
                .append(",");
        List<String> names = filterFromTableSchema.getNames();
        sb.append(names.get(0));
        sb.append(",");
        sb.append(names.get(1));
        sb.append(")");
    }

    @Override
    public void visit(FromRelToSqlSchema fromRelSchema) {
        String targetName = fromRelSchema.getTargetName();
        sb.append(fromRelSchema.getOp().getFun())
                .append("(")
                .append(targetName)
                .append(",");
        fromRelSchema.getRel().accept(this);
        sb.append(")");
    }


    public String getString() {
        return sb.toString();
    }
}
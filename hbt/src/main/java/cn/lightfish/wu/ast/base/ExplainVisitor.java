package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.AggregateCall;
import cn.lightfish.wu.ast.Direction;
import cn.lightfish.wu.ast.modify.ModifyTable;
import cn.lightfish.wu.ast.query.*;
import org.apache.calcite.avatica.util.ByteString;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

public class ExplainVisitor implements NodeVisitor {
    final StringBuilder sb = new StringBuilder();

    @Override
    public void visit(MapSchema mapSchema) {
        Schema schema = mapSchema.getSchema();
        List<Expr> expr = mapSchema.getExpr();
        sb.append("map(");
        schema.accept(this);
        sb.append(",");
        joinNode(expr);
        sb.append(")");
    }


    public String aggregateOrder(AggregateCall call) {
        String s = aggregateFliter(call);
        if (call.getOrderKeys() != null && !call.getOrderKeys().isEmpty()) {
            return MessageFormat.format("sort({0},{1})", s, orderKeys(call.getOrderKeys()));
        } else {
            return s;
        }
    }

    public String aggregateFliter(AggregateCall call) {
        String s = aggregateIgnoreNulls(call);
        if (call.getFilter() != null) {
            return MessageFormat.format("filter({0},{1})", s, getExprString(call.getFilter()));
        } else {
            return s;
        }
    }

    private String aggregateIgnoreNulls(AggregateCall call) {
        String s = aggregateApproximate(call);
        Boolean ignoreNulls = call.getIgnoreNulls();
        if (ignoreNulls == Boolean.TRUE) {
            return MessageFormat.format("ignoreNulls({0})", s);
        } else {
            return s;
        }
    }

    private String aggregateApproximate(AggregateCall call) {
        String s = aggregateDistinct(call);
        Boolean approximate = call.getApproximate();
        if (approximate == Boolean.TRUE) {
            return MessageFormat.format("approximate({0})", s);
        } else {
            return s;
        }
    }

    private String aggregateDistinct(AggregateCall call) {
        String s = aggregateAs(call);
        Boolean distinct = call.getDistinct();
        if (distinct == Boolean.TRUE) {
            return MessageFormat.format("distinct({0})", s);
        } else {
            return s;
        }
    }

    private String aggregateAs(AggregateCall call) {
        String s = aggregateArgs(call);
        if (call.getAlias() != null) {
            return MessageFormat.format("as({0},{1})", s, toId(call.getAlias()));
        }
        return s;
    }

    private String aggregateArgs(AggregateCall call) {
        List<Expr> operands = call.getOperands();
        if (operands != null && !operands.isEmpty()) {
            String args = operands.stream().map(i -> getExprString(i)).collect(Collectors.joining(","));
            return MessageFormat.format("call({0},{1})", toId(call.getFunction()), args);
        } else {
            return MessageFormat.format("call({0})", toId(call.getFunction()));
        }
    }

    private String getExprString(Expr i) {
        ExplainVisitor explainVisitor = new ExplainVisitor();
        i.accept(explainVisitor);
        return explainVisitor.getSb();
    }


    @Override
    public void visit(GroupSchema groupSchema) {
        List<AggregateCall> exprs = groupSchema.getExprs();
        List<GroupItem> keys = groupSchema.getKeys();
        sb.append("group(");
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
            Identifier columnName = orderItem.getColumnName();
            sb.append(columnName.getValue());
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
            Op op = key.getOp();
            if (op == Op.REGULAR) {
                sb.append("regular(");
                joinNode(key.getExprs());
                sb.append(")");
            } else {
                throw new UnsupportedOperationException();
            }
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
        Literal offset = limitSchema.getOffset();
        sb.append(offset.getValue());
        sb.append(",");
        Literal limit = limitSchema.getLimit();
        sb.append(limit.getValue());
        sb.append(")");
    }

    @Override
    public void visit(FromSchema fromSchema) {
        sb.append("from(");
        sb.append(fromSchema.getNames().stream().map(i -> toId(i.getValue())).collect(Collectors.joining(",")));
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
            target = "literal('" + value + "')";
        } else if (value instanceof byte[]) {
            byte[] value1 = (byte[]) value;
            ByteString byteString = new ByteString(value1);
            target = "literal(X'" + byteString.toString() + "')";
        } else if (value instanceof Number) {
            target = "literal(" + value + ")";
        } else if (value instanceof LocalDate) {
            target = "dateLiteral(" + (value) + ")";
        } else if (value instanceof LocalDateTime) {
            target = "timestampLiteral(" + (value) + ")";
        } else if (value instanceof LocalTime) {
            target = "timeLiteral(" + (value) + ")";
        } else {
            target = "literal(" + value + ")";
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
            sb.append(orders.stream().map(i -> {
                Identifier columnName = i.getColumnName();
                String name = i.getDirection().name();
                return "order(" + toId(columnName.getValue()) + "," + toId(name) + ")";
            }).collect(Collectors.joining(",")));
            sb.append(")");
        }
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
        sb.append("valuesSchema(");
        sb.append("fields(");
        joinNode(valuesSchema.getFieldNames());
        sb.append(")");
        sb.append(",");
        sb.append("values(");
        joinNode(valuesSchema.getValues());
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
        sb.append("join").append("(")
                .append(corJoinSchema.getOp().getFun());
        if (condition != null) {
            sb.append(",")
                    .append(getExprString(condition));
        }

        sb.append(",");
        joinNode(corJoinSchema.getSchemas());
        sb.append(")");
    }


    @Override
    public void visit(AggregateCall aggregateCall) {
        sb.append(aggregateOrder(aggregateCall));
    }

    @Override
    public void visit(FilterSchema filterSchema) {
        sb.append("filter(");
        filterSchema.getSchema().accept(this);
        sb.append(",");
        joinNode(filterSchema.getExprs());
        sb.append(")");
    }

    @Override
    public void visit(ModifyTable modifyTable) {

    }

    @Override
    public void visit(DistinctSchema distinctSchema) {

    }

    @Override
    public void visit(ProjectSchema projectSchema) {
        List<String> columnNames = projectSchema.getColumnNames();
        sb.append("projectNamed(");
        projectSchema.getSchema().accept(this);
        sb.append(",");
        sb.append(columnNames.stream().map(this::toId).collect(Collectors.joining(",")));
        sb.append(")");
    }

    private String toId(String i) {
        return getExprString(new Identifier(i));
    }

    @Override
    public void visit(CorrelateSchema correlate) {
        sb.append(correlate.getOp().getFun())
                .append("(")
                .append(getExprString(correlate.getRefName()))
                .append(",").append("keys(").append(correlate.getColumnName().stream().map(i -> getExprString(i)).collect(Collectors.joining(",")))
                .append(")")
                .append(",");
        correlate.getLeft().accept(this);
        sb.append(",");
        correlate.getRight().accept(this);
        sb.append(")");
    }

    public String getSb() {
        return sb.toString();
    }
}
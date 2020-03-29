package io.mycat.hbt;

import io.mycat.hbt.ast.HBTOp;
import io.mycat.hbt.ast.base.Literal;
import io.mycat.hbt.ast.base.*;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.hbt.ast.query.*;
import io.mycat.hbt.parser.CallExpr;
import io.mycat.hbt.parser.ParenthesesExpr;
import io.mycat.hbt.parser.ParseNode;
import io.mycat.hbt.parser.literal.*;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;


public class SchemaConvertor {
    private int index = 0;
    private final List<Object> params;
    static final Map<String, HBTOp> map = new HashMap<>();

    static {
        for (HBTOp value : HBTOp.values()) {
            map.put(value.getFun().toLowerCase(), value);
        }
    }

    public SchemaConvertor() {
        this(Collections.emptyList());
    }

    public SchemaConvertor(List<Object> params) {
        this.params = params;
    }

    public Expr transforExpr(ParseNode parseNode) {
        if (parseNode instanceof CallExpr) {
            CallExpr parseNode1 = (CallExpr) parseNode;
            String name = parseNode1.getName();
            List<ParseNode> exprs = parseNode1.getArgs().getExprs();
            List<Expr> collect = exprs.stream().map(i -> transforExpr(i)).collect(Collectors.toList());
            return new Fun(name, collect);
        } else if (parseNode instanceof DecimalLiteral) {
            return new Literal(((DecimalLiteral) parseNode).getNumber());
        } else if (parseNode instanceof IdLiteral) {
            return new Identifier(((IdLiteral) parseNode).getId());
        } else if (parseNode instanceof StringLiteral) {
            return new Literal(((StringLiteral) parseNode).getString());
        } else if (parseNode instanceof IntegerLiteral) {
            return new Literal(((IntegerLiteral) parseNode).getNumber());
        } else if (parseNode instanceof ParenthesesExpr) {
            ParenthesesExpr parseNode1 = (ParenthesesExpr) parseNode;
            List<ParseNode> exprs = parseNode1.getExprs();
            if (exprs.size() == 1) {
                return transforExpr(exprs.get(0));
            }
        } else if (parseNode instanceof BooleanLiteral) {
            return new Literal(((BooleanLiteral) parseNode).getValue());
        } else if (parseNode instanceof NullLiteral) {
            return new Literal(null);
        } else if (parseNode instanceof ParamLiteral) {
            if (params.isEmpty()) {
                return new Param();
            }
            return new Literal(params.get(index++));
        }
        throw new UnsupportedOperationException();
    }

    public FieldType fieldType(ParseNode parseNode) {
        CallExpr parseNode1 = (CallExpr) parseNode;
        String name = parseNode1.getName();
        List<ParseNode> exprs = parseNode1.getArgs().getExprs();
        String id = exprs.get(0).toString();
        String type = exprs.get(1).toString();
        final boolean nullable = Optional.ofNullable(getArg(exprs, 2)).map(i -> Boolean.parseBoolean(i)).orElse(true);
        final Integer precision = Optional.ofNullable(getArg(exprs, 3)).map(i -> Integer.parseInt(i)).orElse(null);
        final Integer scale = Optional.ofNullable(getArg(exprs, 4)).map(i -> Integer.parseInt(i)).orElse(null);
        return new FieldType(id, type, nullable, precision, scale);
    }

    public String getArg(List<ParseNode> exprs, int index) {
        if (exprs.size() > index) {
            return exprs.get(index).toString();
        } else {
            return null;
        }
    }



    public List<FieldType> fields(ParseNode fields) {
        CallExpr callExpr = (CallExpr) fields;
        List<ParseNode> exprs = callExpr.getArgs().getExprs();
        return exprs.stream().map(i -> fieldType(i)).collect(Collectors.toList());
    }

    public List<Object> values(ParseNode fields) {
        CallExpr callExpr = (CallExpr) fields;
        List<ParseNode> exprs = callExpr.getArgs().getExprs();
        return exprs.stream().map(i -> ((Literal) transforExpr(i)).getValue()).collect(Collectors.toList());
    }


    public Schema transforSchema(ParseNode parseNode) {
        if (parseNode instanceof CallExpr) {
            CallExpr node = (CallExpr) parseNode;
            String name = node.getName().toLowerCase();
            List<ParseNode> exprList = node.getArgs().getExprs();
            HBTOp op = map.get(name);
            if (op == null) {
                System.err.println(name);
                Objects.requireNonNull(name);
            }
            switch (op) {
                case UNION_DISTINCT:
                case UNION_ALL:
                case EXCEPT_DISTINCT:
                case EXCEPT_ALL:
                case INTERSECT_DISTINCT:
                case INTERSECT_ALL: {
                    List<Schema> collect = exprList.stream().map(expr -> transforSchema(expr)).collect(Collectors.toList());
                    return set(op, collect);
                }
                case FROM_TABLE: {
                    List<String> collect = exprList.stream().map(i -> i.toString()).collect(Collectors.toList());
                    return fromTable(collect);
                }
                case FROM_REL_TO_SQL: {
                    Schema schema = transforSchema(exprList.get(1));
                    return new FromRelToSqlSchema(exprList.get(0).toString(), schema);
                }
                case FROM_SQL: {
                    List<FieldType> fieldTypes;
                    String targetName = null;
                    String sql = null;
                    switch (exprList.size()) {
                        case 2: {
                            targetName = exprList.get(0).toString();
                            fieldTypes = Collections.emptyList();
                            sql = exprList.get(1).toString();
                            break;
                        }
                        case 3: {
                            targetName = exprList.get(0).toString();
                            fieldTypes = fields(exprList.get(1));
                            sql = exprList.get(2).toString();
                            break;
                        }
                        default:
                            throw new IllegalArgumentException();
                    }
                    return new FromSqlSchema(fieldTypes, targetName, sql);
                }
                case FILTER_FROM_TABLE: {
                    List<String> collect = exprList.subList(1, exprList.size()).stream().map(i -> i.toString()).collect(Collectors.toList());
                    return new FilterFromTableSchema(transforExpr(exprList.get(0)), collect);
                }
                case MAP: {
                    List<Expr> collect = exprList.subList(1, exprList.size()).stream().map(i -> transforExpr(i)).collect(Collectors.toList());
                    Schema schema = transforSchema(exprList.get(0));
                    return map(schema, collect);
                }
                case FILTER: {
                    Schema schema = transforSchema(exprList.get(0));
                    Expr expr = transforExpr(exprList.get(1));
                    return filter(schema, expr);
                }
                case LIMIT: {
                    Schema schema = transforSchema(exprList.get(0));
                    Number offset = getNumber(exprList.get(1));
                    Number limit = getNumber(exprList.get(2));
                    return limit(schema, offset, limit);
                }
                case ORDER: {
                    List<OrderItem> orderItemList = order(exprList.subList(1, exprList.size()));
                    Schema schema = transforSchema(exprList.get(0));
                    return orderBy(schema, orderItemList);
                }
                case GROUP: {
                    int size = exprList.size();
                    CallExpr source = (CallExpr) exprList.get(0);
                    CallExpr keys = (CallExpr) exprList.get(1);
                    Schema schema = transforSchema(source);
                    List<AggregateCall> aggregating = Collections.emptyList();
                    List<GroupKey> groupkeys = keys(keys);
                    switch (size) {
                        case 2: {
                            break;
                        }
                        case 3: {
                            CallExpr exprs = (CallExpr) exprList.get(2);
                            aggregating = aggregating(exprs);
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException();
                    }
                    return groupBy(schema, groupkeys, aggregating);
                }
                case TABLE: {
                    List<FieldType> fields = fields(exprList.get(0));
                    List<Object> values = values(exprList.get(1));
                    return table(fields, values);
                }
                case DISTINCT: {
                    Schema schema = transforSchema(exprList.get(0));
                    return distinct(schema);
                }
                case RENAME: {
                    Schema schema = transforSchema(exprList.get(0));
                    List<String> iterms = exprList.subList(1, exprList.size()).stream().map(i -> i.toString()).collect(Collectors.toList());
                    return new RenameSchema(schema, iterms);
                }
                case INNER_JOIN:
                case LEFT_JOIN:
                case RIGHT_JOIN:
                case FULL_JOIN:
                case SEMI_JOIN:
                case ANTI_JOIN: {
                    Expr expr = transforExpr(exprList.get(0));
                    Schema schema = transforSchema(exprList.get(1));
                    Schema schema2 = transforSchema(exprList.get(2));

                    return join(op, expr, schema, schema2);
                }
                case CORRELATE_INNER_JOIN:
                case CORRELATE_LEFT_JOIN: {
                    String refName = exprList.get(0).toString();
                    Schema leftschema = transforSchema(exprList.get(1));
                    Schema rightschema = transforSchema(exprList.get(2));
                    return correlate(op, refName, leftschema, rightschema);
                }
                case EXPLAIN: {
                    Schema schema = transforSchema(((CallExpr) (parseNode)).getArgs().getExprs().get(0));
                    return new CommandSchema(HBTOp.EXPLAIN, schema);
                }
                case EXPLAIN_SQL: {
                    Schema schema = transforSchema(((CallExpr) (parseNode)).getArgs().getExprs().get(0));
                    return new CommandSchema(HBTOp.EXPLAIN_SQL, schema);
                }
                case MODIFY_FROM_SQL:{
                  return   new ModifyFromSql(exprList.get(0).toString(),exprList.get(1).toString());
                }
                case MERGE_MODIFY:{
                    return new MergeModify(exprList.stream().map(i -> (ModifyFromSql) transforSchema(i)).collect(Collectors.toList()));
                }
                default: {
                    throw new UnsupportedOperationException();
                }
            }
        } else if (parseNode instanceof ParenthesesExpr) {
            ParenthesesExpr parseNode1 = (ParenthesesExpr) parseNode;
            if (parseNode1.getExprs().size() == 1) {
                return transforSchema(parseNode1.getExprs().get(0));
            } else {
                System.out.println();
            }
        } else {
            return null;
        }
        return null;
    }

    @NotNull
    public Schema filter(Schema schema, Expr expr) {
        return new FilterSchema(schema, expr);
    }

    @NotNull
    public Schema correlate(HBTOp op, String refName, Schema leftschema, Schema rightschema) {
        return new CorrelateSchema(op, refName, leftschema, rightschema);
    }

    @NotNull
    public Schema join(HBTOp op, Expr expr, Schema left, Schema right) {
        return new JoinSchema(op, expr, left, right);
    }

//    @NotNull
//    public  Schema rename(Schema schema, List<String> iterms) {
//        return new RenameSchema(schema, iterms);
//    }

    @NotNull
    public Schema distinct(Schema schema) {
        return new DistinctSchema(schema);
    }

    @NotNull
    public Schema table(List<FieldType> fields, List<Object> values) {
        return new AnonyTableSchema(fields, values);
    }


    @NotNull
    public Schema groupBy(Schema schema, List<GroupKey> groupkeys, List<AggregateCall> aggregating) {
        return new GroupBySchema(schema, groupkeys, aggregating);
    }

    @NotNull
    public Schema orderBy(Schema schema, List<OrderItem> orderItemList) {
        return new OrderSchema(schema, orderItemList);
    }

    @NotNull
    public Schema limit(Schema schema, Number offset, Number limit) {
        return new LimitSchema(schema, offset, limit);
    }


    @NotNull
    public Schema map(Schema schema, List<Expr> collect) {
        return new MapSchema(schema, collect);
    }

    public Schema fromTable(String schema, String table) {
        return fromTable(Arrays.asList(schema, table));
    }

    public Schema fromTable(List<String> collect) {
        return new FromTableSchema(collect);
    }

    @NotNull
    public Schema set(HBTOp op, List<Schema> collect) {
        return new SetOpSchema(op, collect);
    }


    @NotNull
    public List<OrderItem> order(List<ParseNode> exprList) {
        return exprList.stream().map(i -> getOrderItem(i)).collect(Collectors.toList());
    }

    public List<AggregateCall> aggregating(CallExpr exprs) {
        return exprs.getArgs().getExprs().stream().map(i -> aggregateCall(i)).collect(Collectors.toList());
    }

    public AggregateCall aggregateCall(ParseNode parseNode) {
        CallExpr callExpr = (CallExpr) parseNode;

        List<ParseNode> exprs = Collections.emptyList();
        List<ParseNode> orderBy = new ArrayList<>();
        ParseNode filter = null;
        Boolean ignoreNulls = null;
        Boolean approximate = null;
        Boolean distinct = null;
        String alias = null;
        String name = null;

        while (true) {
            name = callExpr.getName();
            exprs = callExpr.getArgs().getExprs();
            if ("orderBy".equalsIgnoreCase(name)) {
                orderBy.addAll(exprs.subList(1, exprs.size()));
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            if ("filter".equalsIgnoreCase(name)) {
                filter = exprs.get(1);
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            if ("ignoreNulls".equalsIgnoreCase(name)) {
                ignoreNulls = true;
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            if ("approximate".equalsIgnoreCase(name)) {
                approximate = true;
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            if ("distinct".equalsIgnoreCase(name)) {
                distinct = true;
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            if ("alias".equalsIgnoreCase(name)) {
                alias = exprs.get(1).toString();
                callExpr = (CallExpr) exprs.get(0);
                continue;
            }
            break;
        }
        List<Expr> collect = callExpr.getArgs().getExprs().stream().map(i -> transforExpr(i)).collect(Collectors.toList());
        Expr filterExpr = null;
        if (filter != null) {
            filterExpr = transforExpr(filter);
        }
        return new AggregateCall(callExpr.getName(), alias, collect, distinct, approximate, ignoreNulls, filterExpr, orderBy.stream().map(i -> getOrderItem(i)).collect(Collectors.toList()));
    }

    public List<GroupKey> keys(CallExpr keys) {
        List<ParseNode> exprs = keys.getArgs().getExprs();
        return exprs.stream().map(i -> getGroupItem(i)).collect(Collectors.toList());
    }

    public GroupKey getGroupItem(ParseNode parseNode) {
        CallExpr groupKey = (CallExpr) parseNode;
        List<Expr> collect = groupKey.getArgs().getExprs().stream().map(i -> transforExpr(i)).collect(Collectors.toList());
        return groupkey(collect);
    }


    public GroupKey groupkey(List<Expr> exprs) {
        return new GroupKey(exprs);
    }

    public OrderItem getOrderItem(ParseNode parseNode) {
        CallExpr parseNode1 = (CallExpr) parseNode;
        List<ParseNode> exprs = parseNode1.getArgs().getExprs();
        String identifier = exprs.get(0).toString();
        Direction direction = Direction.parse(exprs.get(1).toString());
        return order(identifier, direction);
    }


    public OrderItem order(String identifier, Direction direction) {
        return new OrderItem(identifier, direction);
    }

    public Number getNumber(ParseNode parseNode) {
        if (parseNode instanceof IntegerLiteral) {
            return ((IntegerLiteral) parseNode).getNumber();
        }
        if (parseNode instanceof DecimalLiteral) {
            return ((DecimalLiteral) parseNode).getNumber();
        }
        return (new BigDecimal(parseNode.toString()));
    }

}

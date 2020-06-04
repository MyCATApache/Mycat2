package io.mycat.route;

import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.query.*;

public class HBTQueryConvertor2 {
    public ResultHandler complie(Schema root) {
        return handle(root);
    }

    private ResultHandler handle(Schema input) {
        try {
            switch (input.getOp()) {
                case FROM_TABLE:
                    return fromTable((FromTableSchema) input);
                case FROM_SQL:
                    return fromSql((FromSqlSchema) input);
                case FROM_REL_TO_SQL: {
                    return fromRelToSqlSchema((FromRelToSqlSchema) input);
                }
                case FILTER_FROM_TABLE: {
                    return filterFromTable((FilterFromTableSchema) input);
                }
                case MAP:
                    return map((MapSchema) input);
                case FILTER:
                    return filter((FilterSchema) input);
                case LIMIT:
                    return limit((LimitSchema) input);
                case ORDER:
                    return order((OrderSchema) input);
                case GROUP:
                    return group((GroupBySchema) input);
                case TABLE:
                    return values((AnonyTableSchema) input);
                case DISTINCT:
                    return distinct((DistinctSchema) input);
                case UNION_ALL:
                case UNION_DISTINCT:
                case EXCEPT_ALL:
                case EXCEPT_DISTINCT:
                case INTERSECT_DISTINCT:
                case INTERSECT_ALL:
                    return setSchema((SetOpSchema) input);
                case LEFT_JOIN:
                case RIGHT_JOIN:
                case FULL_JOIN:
                case SEMI_JOIN:
                case ANTI_JOIN:
                case INNER_JOIN:
                    return correlateJoin((JoinSchema) input);
                case RENAME:
                    return rename((RenameSchema) input);
                case CORRELATE_INNER_JOIN:
                case CORRELATE_LEFT_JOIN:
                    return correlate((CorrelateSchema) input);
                default:
            }
        } finally {

        }
        throw new UnsupportedOperationException(input.getOp().getFun());

    }

    private ResultHandler correlate(CorrelateSchema input) {
        return null;
    }

    private ResultHandler rename(RenameSchema input) {
        return null;
    }

    private ResultHandler correlateJoin(JoinSchema input) {
        return null;
    }

    private ResultHandler setSchema(SetOpSchema input) {
        return null;
    }

    private ResultHandler distinct(DistinctSchema input) {
        return null;
    }

    private ResultHandler values(AnonyTableSchema input) {
        return null;
    }

    private ResultHandler group(GroupBySchema input) {
        return null;
    }

    private ResultHandler order(OrderSchema input) {
        return null;
    }

    private ResultHandler limit(LimitSchema input) {
        return null;
    }

    private ResultHandler filter(FilterSchema input) {
        return null;
    }

    private ResultHandler map(MapSchema input) {
        return null;
    }

    private ResultHandler filterFromTable(FilterFromTableSchema input) {
        return null;
    }

    private ResultHandler fromRelToSqlSchema(FromRelToSqlSchema input) {
        return null;
    }

    private InputHandler fromSql(FromSqlSchema input) {
        String targetName = input.getTargetName();
        String sql = input.getSql();
        return new InputHandler(targetName,sql);
    }

    private ResultHandler fromTable(FromTableSchema input) {

        return null;
    }
}
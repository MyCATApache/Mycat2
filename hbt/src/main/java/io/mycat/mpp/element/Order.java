package io.mycat.mpp.element;

import com.alibaba.fastsql.sql.ast.SQLOrderingSpecification;
import io.mycat.mpp.SqlValue;
import lombok.Getter;

@Getter
public class Order {
    private final SqlValue sqlAbs;
    private final SQLOrderingSpecification type;

    public Order(SqlValue sqlAbs, SQLOrderingSpecification type) {
        this.sqlAbs = sqlAbs;
        this.type = type;
    }
}
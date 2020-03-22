/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.prepare;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PlanRunner;
import lombok.Getter;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

/**
 * @author Junwen Chen
 **/
@Getter
public class MycatCalciteSQLPrepareObject extends MycatSQLPrepareObject {
    private final SqlNode sqlNode;
    private final MycatRowMetaData parameterRowType;
    private final MycatRowMetaData resultSetRowType;
    private boolean forUpdate;
    private final MycatDBContext dataContext;

    public MycatCalciteSQLPrepareObject(Long id, String sql, SqlNode sqlNode, MycatRowMetaData parameterRowType, MycatRowMetaData resultSetRowType, boolean forUpdate, MycatDBContext dataContext) {
        super(id,dataContext,sql,forUpdate);
        this.sqlNode = sqlNode;
        this.parameterRowType = parameterRowType;
        this.resultSetRowType = resultSetRowType;
        this.forUpdate = forUpdate;
        this.dataContext = dataContext;
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return parameterRowType;
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        return resultSetRowType;
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        SqlNode accept = params.isEmpty() ? sqlNode : SqlNode.clone(sqlNode).accept(
                new SqlShuttle() {
                    int index = 0;

                    @Override
                    public SqlNode visit(SqlDynamicParam param) {
                        Object o = params.get(index);
                        index++;
                        return literal(o);
                    }
                });
        return new MycatSqlPlanner(this, accept.toSqlString(MysqlSqlDialect.DEFAULT).getSql(),dataContext);
    }

    public static SqlNode literal(Object value) {
        SqlParserPos zero = SqlParserPos.ZERO;
        SqlNode literal;
        if (value == null) {
            literal = SqlLiteral.createNull(zero);
        } else if (value instanceof Boolean) {
            literal = SqlLiteral.createBoolean((Boolean) value, zero);
        } else if (value instanceof BigDecimal) {
            literal = SqlLiteral.createExactNumeric(((BigDecimal) value).toPlainString(), zero);
        } else if (value instanceof Number) {
            literal = SqlLiteral.createExactNumeric(value.toString(), zero);
        } else if (value instanceof String) {
            literal = SqlLiteral.createCharString((String) value, zero);
        } else if (value instanceof byte[]) {
            literal = SqlLiteral.createBinaryString((byte[]) value, zero);
        } else if (value instanceof LocalDate) {
            LocalDate value1 = (LocalDate) value;
            DateString dateString = new DateString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth());
            literal = SqlLiteral.createDate(dateString, zero);
        } else if (value instanceof LocalTime) {
            LocalTime value1 = (LocalTime) value;
            TimeString timeString = new TimeString(value1.getHour(), value1.getMinute(), value1.getSecond());
            literal = SqlLiteral.createTime(timeString, 64, zero);
        } else if (value instanceof LocalDateTime) {
            LocalDateTime value1 = (LocalDateTime) value;
            TimestampString timeString = new TimestampString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth(), value1.getHour(), value1.getMinute(), value1.getSecond());
            timeString = timeString.withNanos(value1.getNano());
            literal = SqlLiteral.createTimestamp(timeString, 64, zero);
        } else {
            throw new IllegalArgumentException("cannot convert " + value
                    + " (" + value.getClass() + ") to a constant");
        }
        return literal;
    }

    public boolean isForUpdate() {
        return forUpdate;
    }
}
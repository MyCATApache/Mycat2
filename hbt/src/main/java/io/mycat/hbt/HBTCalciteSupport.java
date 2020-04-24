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

import com.google.common.collect.HashBiMap;
import io.mycat.hbt.parser.HBTParser;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author jamie12221
 **/
public enum HBTCalciteSupport {
    INSTANCE;
    private final HashBiMap<String, SqlAggFunction> sqlAggFunctionMap;
    private final HashBiMap<String, SqlOperator> sqlOperatorMap;
    private final HashBiMap<String, SqlTypeName> typeMap;
    private final HashBiMap<Integer, SqlTypeName> jdbcValueMap;
    private final HashBiMap<String, Class> type2ClassMap;
    private final HashBiMap<SqlTypeName, Class> sqlType2ClassMap;
     final Map<String, HBTParser.Precedence> operators;

    public SqlAggFunction getAggFunction(String name) {
        return Objects.requireNonNull(sqlAggFunctionMap.get(name),name);
    }

    public String getAggFunctionName(SqlAggFunction aggFunction) {
        return Objects.requireNonNull(sqlAggFunctionMap.inverse().get(aggFunction),aggFunction.toString());
    }

    public SqlOperator getSqlOperator(String name) {
        return Objects.requireNonNull(sqlOperatorMap.get(name),name);
    }

    public String getSqlOperatorName(SqlOperator operator) {
        return Objects.requireNonNull(sqlOperatorMap.inverse().get(operator),operator.toString());
    }

    public SqlTypeName getSqlTypeName(String name) {
        return Objects.requireNonNull(typeMap.get(name.toLowerCase()),""+name);
    }
    public SqlTypeName getSqlTypeByJdbcValue(int value) {
        return Objects.requireNonNull(jdbcValueMap.get(value));
    }

    public String getSqlTypeName(SqlTypeName name) {
        return Objects.requireNonNull(typeMap.inverse().get(name),name.toString());
    }

    public Class getJavaClass(String name) {
        return Objects.requireNonNull(type2ClassMap.get(name),name);
    }

    public Class getJavaClass(SqlTypeName name) {
        return Objects.requireNonNull(sqlType2ClassMap.get(name),name.toString());
    }

    public void addOperator(String op, String opText, int value, boolean leftAssoc) {
        operators.put(op, new HBTParser.Precedence(opText, value, leftAssoc));
        operators.put(opText, new HBTParser.Precedence(opText, value, leftAssoc));
    }

    public void addOperator(String op, int value, boolean leftAssoc) {
        addOperator(op, op, value, leftAssoc);
    }

    HBTCalciteSupport() {
        typeMap = HashBiMap.create();
        type2ClassMap = HashBiMap.create();
        sqlType2ClassMap = HashBiMap.create();
        sqlAggFunctionMap = HashBiMap.create();
        sqlOperatorMap = HashBiMap.create();
        jdbcValueMap = HashBiMap.create();

        sqlAggFunctionMap.put("avg", SqlStdOperatorTable.AVG);
        sqlAggFunctionMap.put("count", SqlStdOperatorTable.COUNT);
        sqlAggFunctionMap.put("max", SqlStdOperatorTable.MAX);
        sqlAggFunctionMap.put("min", SqlStdOperatorTable.MIN);
        sqlAggFunctionMap.put("sum", SqlStdOperatorTable.SUM);

        sqlOperatorMap.put("eq", SqlStdOperatorTable.EQUALS);
        sqlOperatorMap.put("ne", SqlStdOperatorTable.NOT_EQUALS);
        sqlOperatorMap.put("gt", SqlStdOperatorTable.GREATER_THAN);
        sqlOperatorMap.put("gte", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        sqlOperatorMap.put("lt", SqlStdOperatorTable.LESS_THAN);
        sqlOperatorMap.put("lte", SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        sqlOperatorMap.put("and", SqlStdOperatorTable.AND);
        sqlOperatorMap.put("or", SqlStdOperatorTable.OR);
        sqlOperatorMap.put("not", SqlStdOperatorTable.NOT);
        sqlOperatorMap.put("add", SqlStdOperatorTable.PLUS);
        sqlOperatorMap.put("minus", SqlStdOperatorTable.MINUS);
        sqlOperatorMap.put("dot", SqlStdOperatorTable.DOT);

        sqlOperatorMap.put("lower", SqlStdOperatorTable.LOWER);

        sqlOperatorMap.put("upper", SqlStdOperatorTable.UPPER);

        sqlOperatorMap.put("round", SqlStdOperatorTable.ROUND);

        sqlOperatorMap.put("isnull", SqlStdOperatorTable.IS_NULL);

        sqlOperatorMap.put("nullif", SqlStdOperatorTable.NULLIF);
        sqlOperatorMap.put("isnotnull", SqlStdOperatorTable.IS_NOT_NULL);
        sqlOperatorMap.put("cast", SqlStdOperatorTable.CAST);
        sqlOperatorMap.put("substring", SqlStdOperatorTable.SUBSTRING);

        for (SqlTypeName value : SqlTypeName.values()) {
            putTypeName(value.getName().toLowerCase(), value);
            putJdbcValue(value.getJdbcOrdinal(), value);
        }

//        put("long", SqlTypeName.DECIMAL);
//        put("boolean", SqlTypeName.BOOLEAN);
//
//        put("int", SqlTypeName.DECIMAL);
//        put("int", SqlTypeName.INTEGER);
//        put("float", SqlTypeName.FLOAT);
//        put("double", SqlTypeName.DOUBLE);
//        put("long", SqlTypeName.BIGINT);
//        put("date", SqlTypeName.DATE);
//        put("time", SqlTypeName.TIME);
//        put("timestamp", SqlTypeName.TIMESTAMP);
//        put("varbinary", SqlTypeName.VARBINARY);
//        put("varchar", SqlTypeName.VARCHAR);

        operators = new HashMap<>();


        ///////////////////////////////object/////////////////////////.
        addOperator(".", "dot", 19, true);
        addOperator("dot", 19, true);

        addOperator("+", "add", 13, true);
        addOperator("add", 13, true);
        addOperator("-", "minus", 13, true);
        addOperator("minus", 13, true);

        addOperator("=", "eq", 10, true);
        addOperator("eq", 10, true);

        addOperator(">", "gt", 11, true);
        addOperator("gt", 11, true);
        addOperator(">=", "gte", 11, true);
        addOperator("gte", 11, true);

        addOperator("<", "lt", 11, true);
        addOperator("lt", 11, true);
        addOperator("<=", "lte", 11, true);
        addOperator("lte", 11, true);

        ///////////////////////set/////////////////////////////////
        addOperator("unionAll", 1, true);
        addOperator("unionDistinct", 1, true);
        addOperator("exceptDistinct", 1, true);
        addOperator("exceptAll", 1, true);
        addOperator("minusAll", 1, true);
        addOperator("minusDistinct", 1, true);
        addOperator("rename", 1, true);
        addOperator("groupBy", 1, true);
        addOperator("alias", 1, true);
        addOperator("distinct", 1, true);
        addOperator("approximate", 1, true);
        addOperator("ignoreNulls", 1, true);
        addOperator("filter", 1, true);
        addOperator("orderBy", 1, true);


//
//

//
//
        addOperator("!=", "ne", 10, true);
        addOperator("<>", "ne", 10, true);
        addOperator("ne", 10, true);

        addOperator("or", 5, true);
        addOperator("and", 6, true);
        addOperator("as", 3, true);

    }

    private void putJdbcValue(int jdbcOrdinal, SqlTypeName value) {
        jdbcValueMap.put(jdbcOrdinal,value);
    }

    private void putTypeName(String name, SqlTypeName sqlTypeName) {
        typeMap.put(name, sqlTypeName);
    }


    public Map<String, HBTParser.Precedence> getOperators() {
        return operators;
    }
}
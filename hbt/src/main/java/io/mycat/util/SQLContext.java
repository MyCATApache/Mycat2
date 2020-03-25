package io.mycat.util;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface SQLContext {
    Object getSQLVariantRef(String toString);

    List<Object> getParameters();

    void setParameters(List<Object> parameters);

    Map<String, MySQLFunction> functions();

    default String simplySql(String sqlStatement) {
        SQLStatement sqlStatement1 = SQLUtils.parseSingleMysqlStatement(sqlStatement);
        sqlStatement1.accept(new ContextExecuter(this));
        return sqlStatement1.toString();
    }

    default void clearParameters() {
        setParameters(Collections.emptyList());
    }
}
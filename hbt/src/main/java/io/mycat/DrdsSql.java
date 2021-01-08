package io.mycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.beans.mysql.MySQLType;
import lombok.Data;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


@Data
public class DrdsSql {
    private SQLStatement sqlStatement;
    private final String parameterizedString;
    private final List<Object> params;
    private RelNode relNode;
    private List<String> aliasList;
    private final boolean forUpdate;

    public DrdsSql(SQLStatement sqlStatement, String parameterizedString, List<Object> params) {
        this.sqlStatement = sqlStatement;
        this.parameterizedString = parameterizedString;
        this.params = Objects.requireNonNull(params);
        if (sqlStatement instanceof SQLSelectStatement) {
            forUpdate = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock().isForUpdate();
        } else {
            forUpdate = false;
        }
    }

    public List<SqlTypeName> getTypes() {
        if (params == null || params.isEmpty()) return Collections.emptyList();
        if (params.get(0) instanceof List) {
            return getSqlTypeNames((List)params.get(0));
        }else {
            return getSqlTypeNames(params);
        }
    }

    public static List<SqlTypeName> getSqlTypeNames(List<Object> params) {
        ArrayList<SqlTypeName> list = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                list.add(SqlTypeName.NULL);
            } else {
                SqlTypeName sqlTypeName = null;
                for (MySQLType value : MySQLType.values()) {
                    if (value.getJavaClass() == param.getClass()) {
                        sqlTypeName = (SqlTypeName.getNameForJdbcType(value.getJdbcType()));
                        break;
                    }
                }
                list.add(Objects.requireNonNull(sqlTypeName, () -> "unknown type :" + param.getClass()));
            }
        }
        return list;
    }

    public static DrdsSql of(
            SQLStatement sqlStatement,
            String parameterizedString,
            List<Object> params
    ) {
        return new DrdsSql(sqlStatement, parameterizedString, params);
    }

    public static DrdsSql of(
            String parameterizedString,
            List<Object> params
    ) {
        return of(null, parameterizedString, params);
    }

    public <T extends SQLStatement> T getSqlStatement() {
        return (T) (sqlStatement == null ? sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedString) : sqlStatement);
    }

    public void setAliasList(List<String> aliasList) {
        this.aliasList = aliasList;
    }
}
package io.mycat;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import lombok.Data;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Objects;


@Data
public class DrdsSql {
    private  SQLStatement sqlStatement;
    private final String parameterizedString;
    private final List<Object> params;
    private RelNode relNode;
    private List<String> aliasList;

    public DrdsSql(SQLStatement sqlStatement, String parameterizedString, List<Object> params) {
        this.sqlStatement = sqlStatement;
        this.parameterizedString = parameterizedString;
        this.params = Objects.requireNonNull(params);
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
    ){
        return of(null,parameterizedString,params);
    }
    public <T extends SQLStatement> T getSqlStatement() {
        return (T) (sqlStatement == null?sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedString):sqlStatement);
    }

    public void setAliasList(List<String> aliasList) {
        this.aliasList = aliasList;
    }
}
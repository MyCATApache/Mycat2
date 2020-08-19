package io.mycat.hbt3;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@AllArgsConstructor
@Data
public class DrdsSql {
    private  SQLStatement sqlStatement;
    private final String parameterizedString;
    private final List<Object> params;
    private RelNode relNode;

    public static DrdsSql of(
            SQLStatement sqlStatement,
            String parameterizedString,
            List<Object> params
    ) {
        return new DrdsSql(sqlStatement, parameterizedString, params,null);
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
}
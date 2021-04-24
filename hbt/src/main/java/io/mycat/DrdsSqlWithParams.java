package io.mycat;

import io.mycat.calcite.MycatHint;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class DrdsSqlWithParams  extends DrdsSql{
    private final List<Object> params;
    private final List<String> aliasList;

    public DrdsSqlWithParams(String parameterizedSqlStatement,
                             List<Object> params,
                             boolean complex,
                             List<SqlTypeName> typeNames,
                             List<String> aliasList,
                             List<MycatHint> hints) {
        super(parameterizedSqlStatement, complex,typeNames,hints);
        this.params = params;
        this.aliasList = aliasList;
    }

    public List<Object> getParams() {
        return params;
    }

    public List<String> getAliasList() {
        return aliasList;
    }
}

package io.mycat;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class DrdsSqlWithParams  extends DrdsSql{
    private final List<Object> params;
    private final List<String> aliasList;

    public DrdsSqlWithParams(String parameterizedSqlStatement, List<Object> params,boolean complex, List<SqlTypeName> typeNames,List<String> aliasList) {
        super(parameterizedSqlStatement, complex,typeNames);
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

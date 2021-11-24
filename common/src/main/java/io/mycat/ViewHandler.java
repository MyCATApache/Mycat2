package io.mycat;

import java.util.List;

public interface ViewHandler {

    public String getSchemaName() ;

    public String getViewName() ;

    public List<String> getColumns() ;

    public String getViewSql() ;
}

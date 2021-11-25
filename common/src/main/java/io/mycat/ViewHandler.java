package io.mycat;

import io.mycat.config.ViewConfig;

import java.util.List;

public interface ViewHandler {

    public String getSchemaName() ;

    public String getViewName() ;

    public List<String> getColumns() ;

    public String getViewSql() ;

    public ViewConfig getConfig();
}

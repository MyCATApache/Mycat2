package io.mycat.testsuite.tools;

import java.util.List;

public interface SimpleConnection {
    ResultSet executeQuery(String sql);

    void useSchema(String schema);

    interface ResultSet{
        List<String> getColumnList();
        List<Object[]> getRows();
    }
}
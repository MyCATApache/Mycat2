package io.mycat.client;

import java.util.List;

public interface MycatClient {
    public Context analysis(String sql) ;
    public List<String> explain(String sql);
    public void useSchema(String schemaName);
}
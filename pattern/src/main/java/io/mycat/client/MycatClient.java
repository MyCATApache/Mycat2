package io.mycat.client;

import java.util.List;
import java.util.Objects;

public interface MycatClient {
    public Context analysis(String sql) ;
    public List<String> explain(String sql);
    public void useSchema(String schemaName);
}
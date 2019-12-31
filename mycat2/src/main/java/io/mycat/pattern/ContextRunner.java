package io.mycat.pattern;

import io.mycat.client.Context;
import io.mycat.client.MycatClient;

import java.util.*;

public class ContextRunner {

    public static final String USE_STATEMENT = "useStatement";
    public static final String PROXY_DATASOURCE = "proxyDatasource";
    public static final String PROXY_REPLICA = "proxyReplica";
    public List<String> explain() {
        return Collections.singletonList(Objects.toString(this));
    }

    public void run(MycatClient client, Context context) throws IllegalAccessException {
        Map<String, String> names = context.getNames();
        String explain = context.getExplain();
        Map<String, Collection<String>> tables = context.getTables();
        Map<String, String> tags = context.getTags();
        String type = context.getType();
        switch (type) {
            case USE_STATEMENT:{
                String schemaName = Objects.requireNonNull(names.get("schemaName"));
                client.useSchema(schemaName);
                break;
            }
            case PROXY_DATASOURCE:{
                String schemaName = Objects.requireNonNull(names.get("datasourceName"));
                client.useSchema(schemaName);
                break;
            }
            case PROXY_REPLICA:{
                String schemaName = Objects.requireNonNull(names.get("datasourceName"));
                client.useSchema(schemaName);
                break;
            }
        }
    }
}
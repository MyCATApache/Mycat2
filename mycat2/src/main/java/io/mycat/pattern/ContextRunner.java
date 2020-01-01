package io.mycat.pattern;

import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.MySQLTaskUtil;
import io.mycat.proxy.session.MycatSession;

import java.util.*;

public class ContextRunner {

    public static final String USE_STATEMENT = "useStatement";
    public static final String PROXY_DATASOURCE = "proxyDatasource";
    public static final String PROXY_REPLICA = "proxyReplica";
    public static final String SHOW_DATABASES = "showDatabases";

    public List<String> explain() {
        return Collections.singletonList(Objects.toString(this));
    }

    public static void run(MycatClient client, Context context,MycatSession session) {
        Map<String, String> names = context.getNames();
        String explain = context.getExplain();
        Map<String, Collection<String>> tables = context.getTables();
        Map<String, String> tags = context.getTags();

        String type = context.getType();
        switch (type) {
            case USE_STATEMENT: {
                String schemaName = Objects.requireNonNull(names.get("schemaName"));
                client.useSchema(schemaName);
                break;
            }
            case PROXY_DATASOURCE: {
                MySQLTaskUtil.proxyBackendByDatasourceName(session,context.getSql(),context.getVariable("datasourceName"));
                break;
            }
            case PROXY_REPLICA: {
                MySQLTaskUtil.proxyBackendByReplicaName(session,context.getSql(),context.getVariable("replicaName"));
                break;
            }

        }
    }
}
package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.util.NameMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MysqlVariableServiceImpl implements MysqlVariableService {

    //    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);
    private final NameMap<Object> globalVariables;
    private final NameMap<Object> sessionVariables;

    public MysqlVariableServiceImpl(JdbcConnectionManager jdbcConnectionManager) {

        this.globalVariables = new NameMap<Object>();
        this.sessionVariables = new NameMap<>();
        globalVariables.put("tx_isolation", "REPEATABLE-READ");
        globalVariables.put("transaction_isolation","REPEATABLE-READ");

        String prototype = MetadataManager.getPrototype();

        if(MetaClusterCurrent.exist(ReplicaSelectorManager.class)){
            ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            prototype = replicaSelectorManager.getDatasourceNameByReplicaName(prototype, true, null);
        }

        if(jdbcConnectionManager.getConfigAsMap().containsKey(prototype)){
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(prototype)) {
                try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW GLOBAL VARIABLES;")) {
                    while (rowBaseIterator.next()) {
                        globalVariables.put(
                                rowBaseIterator.getString(0),
                                rowBaseIterator.getObject(1)
                        );
                    }
                }
                try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW SESSION VARIABLES;")) {
                    while (rowBaseIterator.next()) {
                        sessionVariables.put(
                                rowBaseIterator.getString(0),
                                rowBaseIterator.getObject(1)
                        );
                    }
                }
            }
        }else {
            globalVariables.put("character_set_client", "utf8");
            globalVariables.put("character_set_connection", "utf8");
            globalVariables.put("character_set_results", "utf8");
            globalVariables.put("character_set_server", "utf8");
            globalVariables.put("init_connect", "");
            globalVariables.put("interactive_timeout", "172800");
            globalVariables.put("lower_case_table_names", "1");
            globalVariables.put("max_allowed_packet", "16777216");
            globalVariables.put("net_buffer_length", "8192");
            globalVariables.put("net_write_timeout", "60");
            globalVariables.put("query_cache_size", "0");
            globalVariables.put("query_cache_type", "OFF");
            globalVariables.put("sql_mode", "STRICT_TRANS_TABLES");
            globalVariables.put("system_time_zone", "CST");
            globalVariables.put("time_zone", "SYSTEM");
            globalVariables.put("lower_case_table_names", "1");
            globalVariables.put("wait_timeout", "172800");
        }

    }

    @Override
    public Object getGlobalVariable(String name) {
        return globalVariables.get(name.startsWith("@@") ? name.substring(2) : name, false);
    }

    @Override
    public Object getSessionVariable(String name) {
        return sessionVariables.get(name, false);
    }

    @Override
    public List<Object[]> getGlobalVariables() {
        LinkedList<Object[]> variables = new LinkedList<>();
        for (Map.Entry<String, Object> e : globalVariables.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            variables.add(new Object[]{key,value});
        }
        return variables;
    }

    @Override
    public List<Object[]> getSessionVariables() {
        LinkedList<Object[]> variables = new LinkedList<>();
        for (Map.Entry<String, Object> e : sessionVariables.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            variables.add(new Object[]{key,value});
        }

        return variables;
    }
}

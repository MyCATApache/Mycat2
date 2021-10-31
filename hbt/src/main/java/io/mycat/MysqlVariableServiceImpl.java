package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.NameMap;

public class MysqlVariableServiceImpl implements MysqlVariableService {

    //    public final SchemaRepository TABLE_REPOSITORY = new SchemaRepository(DbType.mysql);
    private final NameMap<Object> globalVariables;
    private final NameMap<Object> sessionVariables;

    public MysqlVariableServiceImpl(JdbcConnectionManager jdbcConnectionManager) {

        this.globalVariables = new NameMap<Object>();
        this.sessionVariables = new NameMap<>();
        String prototype = MetadataManager.getPrototype();

        try (DefaultConnection connection = jdbcConnectionManager.getConnection(prototype)) {
            try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW GLOBAL VARIABLES;")) {
                while (rowBaseIterator.next()) {
                    globalVariables.put(
                            rowBaseIterator.getString(0),
                            rowBaseIterator.getObject(1)
                    );
                }
            }
        }

        try (DefaultConnection connection = jdbcConnectionManager.getConnection(prototype)) {
            try (RowBaseIterator rowBaseIterator = connection.executeQuery(" SHOW SESSION VARIABLES;")) {
                while (rowBaseIterator.next()) {
                    sessionVariables.put(
                            rowBaseIterator.getString(0),
                            rowBaseIterator.getObject(1)
                    );
                }
            }
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
}

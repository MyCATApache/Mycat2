package io.mycat.upondb;

import com.google.common.collect.ImmutableSet;
import io.mycat.MycatConnection;
import io.mycat.MycatDataContext;
import io.mycat.MycatDataContextEnum;
import io.mycat.RootHelper;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.InformationSchemaRuntime;
import io.mycat.beans.mysql.MySQLVariablesEnum;
import io.mycat.metadata.MetadataManager;
import io.mycat.util.SQLContext;
import io.mycat.util.SQLContextImpl;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class MycatDBs {

    public final static Set<String> VARIABLES_COLUMNNAME_SET = ImmutableSet
            .of("autocommit", "net_write_timeout", "SQL_SELECT_LIMIT", "character_set_results", "read_only", "current_user");


    public static MycatDBClientMediator createClient(MycatDataContext dataContext) {
        return createClient(dataContext, new MycatDBClientBasedConfig(MetadataManager.INSTANCE.getSchemaMap()
                ,Collections.singletonMap("information_schema", InformationSchemaRuntime.INSTANCE.get()
                ),true));
    }

    @NotNull
    public static MycatDBClientMediator createClient(MycatDataContext dataContext, MycatDBClientBasedConfig config) {
        return new MycatDBClientMediator() {
            @Override
            public void setVariable(String target, Object text) {
                String value = Objects.toString(text);
                if (target.contains("autocommit")) {
                    this.setAutoCommit(toInt(value) == 1);
                } else if (target.equalsIgnoreCase("xa")) {
                    int i = toInt(value);
                    if (i == 1) {
                        dataContext.switchTransaction(TransactionType.JDBC_TRANSACTION_TYPE);
                    }
                    if (i == 0) {
                        dataContext.switchTransaction(TransactionType.PROXY_TRANSACTION_TYPE);
                    }
                } else if (target.contains("net_write_timeout")) {
                    dataContext.setVariable(MycatDataContextEnum.NET_WRITE_TIMEOUT, Long.parseLong(value));
                } else if ("SQL_SELECT_LIMIT".equalsIgnoreCase(target)) {
                    dataContext.setVariable(MycatDataContextEnum.SELECT_LIMIT, Long.parseLong(value));
                } else if ("character_set_results".equalsIgnoreCase(target)) {
                    dataContext.setVariable(MycatDataContextEnum.CHARSET_SET_RESULT, value);
                } else if (target.contains("read_only")) {
                    dataContext.setVariable(MycatDataContextEnum.IS_READ_ONLY, toInt(value));
                }
            }

            @Override
            public Object getVariable(String target) {
                target = target.toLowerCase();
                if (target.contains("autocommit")) {
                    return this.isAutoCommit() ? 1 : 0;
                } else if (target.equalsIgnoreCase("xa")) {
                    return dataContext.getTransactionSession().name();
                } else if (target.contains("net_write_timeout")) {
                    return dataContext.getVariable(MycatDataContextEnum.NET_WRITE_TIMEOUT);
                } else if ("sql_select_limit".equalsIgnoreCase(target)) {
                    return dataContext.getVariable(MycatDataContextEnum.SELECT_LIMIT);
                } else if ("character_set_results".equalsIgnoreCase(target)) {
                    return dataContext.getVariable(MycatDataContextEnum.CHARSET_SET_RESULT);
                } else if (target.contains("read_only")) {
                    return dataContext.getVariable(MycatDataContextEnum.IS_READ_ONLY);
                } else if (target.contains("current_user")) {
                    return dataContext.getUser().getUserName();
                }
                Map<String, Object> map = RootHelper.INSTANCE.getConfigProvider().globalVariables();
                MySQLVariablesEnum mySQLVariablesEnum = MySQLVariablesEnum.parseFromColumnName(target);
                if (mySQLVariablesEnum != null) {
                    String columnName = mySQLVariablesEnum.getColumnName();
                    return map.getOrDefault(columnName, null);
                } else {
                    return null;
                }
            }


            @Override
            public MycatDBSharedServer getUponDBSharedServer() {
                return new MycatDBSharedServerImpl();
            }

            @Override
            public MycatDBClientBasedConfig config() {
                return config;
            }

            @Override
            public Map<String, Object> variables() {
                return Collections.emptyMap();
            }

            @Override
            public MycatConnection getConnection(String targetName) {
                return dataContext.getTransactionSession().getConnection(targetName);
            }

            @Override
            public String getSchema() {
                return dataContext.getDefaultSchema();
            }

            @Override
            public void begin() {
                dataContext.getTransactionSession().begin();
            }

            @Override
            public void rollback() {
                dataContext.getTransactionSession().rollback();
            }

            @Override
            public void useSchema(String normalize) {
                dataContext.useShcema(normalize);
            }

            @Override
            public void commit() {
                dataContext.getTransactionSession().commit();
            }

            @Override
            public void setTransactionIsolation(int value) {
                dataContext.getTransactionSession().setTransactionIsolation(value);
            }

            @Override
            public int getTransactionIsolation() {
                return dataContext.getTransactionSession().getTransactionIsolation();
            }

            @Override
            public boolean isAutocommit() {
                return dataContext.isAutocommit();
            }

            @Override
            public void setAutocommit(boolean autocommit) {
                dataContext.setAutoCommit(autocommit);
            }

            @Override
            public boolean isAutoCommit() {
                return dataContext.getTransactionSession().isAutocommit();
            }

            @Override
            public boolean isInTransaction() {
                return dataContext.isInTransaction();
            }

            @Override
            public long getMaxRow() {
                return Integer.MAX_VALUE;
            }

            @Override
            public void setMaxRow(long value) {

            }

            @Override
            public void setAutoCommit(boolean autocommit) {
                dataContext.setAutoCommit(autocommit);
            }

            @Override
            public void close() {

            }

            @Override
            public void recycleResource() {

            }


            @Override
            public int getServerStatus() {
                return dataContext.getTransactionSession().getServerStatus();
            }

            @Override
            public SQLContext sqlContext() {
                return new SQLContextImpl(this);
            }

            @Override
            public long lastInsertId() {
                return dataContext.getLastInsertId();
            }

            @Override
            public AtomicBoolean cancelFlag() {
                return dataContext.getCancelFlag();
            }

            @Override
            public String resolveFinalTargetName(String targetName) {
                return dataContext.resolveDatasourceTargetName(targetName);
            }

            @Override
            public void addCloseResource(AutoCloseable connection) {
                dataContext.getTransactionSession().addCloseResource(connection);
            }

            public MycatDataContext getDataContext() {
                return dataContext;
            }

            int toInt(String s) {
                s = s.trim();
                if ("1".equalsIgnoreCase(s)) {
                    return 1;
                }
                if ("0".equalsIgnoreCase(s)) {
                    return 0;
                }
                if ("on".equalsIgnoreCase(s)) {
                    return 1;
                }
                if ("off".equalsIgnoreCase(s)) {
                    return 0;
                }
                if ("true".equalsIgnoreCase(s)) {
                    return 1;
                }
                if ("false".equalsIgnoreCase(s)) {
                    return 0;
                }
                throw new UnsupportedOperationException(s);
            }

        };
    }
}
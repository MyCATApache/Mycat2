package io.vertx;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.Repository;
import com.alibaba.druid.util.JdbcUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MySQLRepository implements Repository {
    private final static Logger LOGGER = LoggerFactory.getLogger(MySQLRepository.class);
    private final String SELECT_SQL_BY_XID;
    private final String SELECT_SQL_ALL;
    private final String DELETE_SQL;
    private final String REPLACE_SQL;
    private String targetName;
    private final JdbcConnectionManager jdbcConnectionManager;
    private final ReplicaSelectorRuntime replicaSelectorRuntime;
    private final String schemaName;
    private final String tableName;
    private final Executor ASYNC_EXECUTOR;
    private final Cache<String, ImmutableCoordinatorLog> cache;

    public MySQLRepository(String targetName,JdbcConnectionManager jdbcConnectionManager,
                           ReplicaSelectorRuntime replicaSelector,
                           String schemaName,
                           String tableName) {
        this.targetName = targetName;
        this.jdbcConnectionManager = jdbcConnectionManager;
        this.replicaSelectorRuntime = replicaSelector;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ASYNC_EXECUTOR = Executors.newCachedThreadPool();
        this.REPLACE_SQL = MessageFormat.format("REPLACE INTO {0}.{1} (xid,state,expires,info) VALUES (?,?,?);",
                schemaName, tableName);
        this.DELETE_SQL = MessageFormat.format("DELETE FROM {0}.{1} WHERE xid = ?;",
                schemaName, tableName);
        this.SELECT_SQL_ALL = MessageFormat.format("SELECT * FROM {0}.{1};",
                schemaName, tableName);
        this.SELECT_SQL_BY_XID = MessageFormat.format("SELECT * FROM {0}.{1} WHERE xid = ?;",
                schemaName, tableName);

        this.cache = CacheBuilder.newBuilder()
                .initialCapacity(1024)
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void init() {
    }

    @Override
    public void put(String xid, ImmutableCoordinatorLog coordinatorLog) {
        this.ASYNC_EXECUTOR.execute(()->{
            String datasourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);

            try(DefaultConnection connection = jdbcConnectionManager.getConnection(datasourceName)){
                Connection rawConnection = connection.getRawConnection();
                JdbcUtils.execute(
                        rawConnection,
                        this.REPLACE_SQL,
                        Arrays.asList(xid,
                                coordinatorLog.computeMinState(),
                                coordinatorLog.computeExpires(),
                                coordinatorLog.toJson()));
            }catch (Throwable throwable){
                LOGGER.error("");//todo log recover
            }
        });
        cache.put(xid, coordinatorLog);
    }

    @Override
    public void remove(String xid) {
        this.ASYNC_EXECUTOR.execute(() -> {
            try {
                String datasourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
                try(DefaultConnection connection = jdbcConnectionManager.getConnection(datasourceName)){
                    JdbcUtils.execute(
                            connection.getRawConnection(),
                            this.DELETE_SQL,
                            Collections.singletonList(xid));
                }
            } catch (Throwable throwable) {
                LOGGER.error("XA ERROR", throwable);
            }
        });
        cache.invalidate(xid);
    }

    @Override
    public ImmutableCoordinatorLog get(String xid) {
        ImmutableCoordinatorLog log = cache.getIfPresent(xid);
        if (log != null) return log;
        List<Map<String, Object>> maps = null;
        try {
            String datasourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
            try(DefaultConnection connection = jdbcConnectionManager.getConnection(datasourceName)){
                maps = JdbcUtils.executeQuery(
                        connection.getRawConnection(),
                        this.SELECT_SQL_BY_XID,
                        Collections.singletonList(xid));
            }
        } catch (SQLException throwables) {
            LOGGER.error("",throwables);
            return null;
        }
        if (maps.size() == 1) {
            Map<String, Object> map = maps.get(0);
            String info = (String) map.get("info");
            Objects.requireNonNull(info, "info must not null");
            log = ImmutableCoordinatorLog.from(info);
            cache.put(xid, log);
            return log;
        }
        if (maps.size() > 1) {
            throw new IllegalArgumentException("xa log has conflict:\n" +
                    maps.toString());
        }
        return null;

    }

    @Override
    public Collection<ImmutableCoordinatorLog> getCoordinatorLogs() {
        String datasourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);
        try(DefaultConnection connection = jdbcConnectionManager.getConnection(datasourceName)){
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(
                    connection.getRawConnection(),
                    this.SELECT_SQL_ALL, Collections.emptyList());
           return maps.stream().map(s->{
                Object info = s.get("info");
                Objects.requireNonNull(info, "info must not null");
                ImmutableCoordinatorLog log = ImmutableCoordinatorLog.from((String) info);
                return log;
            }).collect(Collectors.toList());
        } catch (SQLException throwables) {
            LOGGER.error("",throwables);
        }
        return Collections.emptyList();
    }


    @Override
    public void close() {
    }
}

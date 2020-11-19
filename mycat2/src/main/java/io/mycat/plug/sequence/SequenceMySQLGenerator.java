package io.mycat.plug.sequence;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.SequenceConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.SplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.function.BiFunction;

public class SequenceMySQLGenerator implements SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(SequenceMySQLGenerator.class);
    private String sql;
    private String queryTargetName;
    private BiFunction<String, String, String> function;
    private long count = 0;
    private long limit = -1;

    public void init(String sql, String targetName) {
        init(sql, targetName, (s, s2) -> {
            ReplicaSelectorRuntime selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            String datasourceName = selectorRuntime.getDatasourceNameByReplicaName(s, true, null);
            JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(datasourceName);
            try (Connection connection1 = jdbcDataSource.getDataSource().getConnection()) {
                try (Statement statement = connection1.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(s2)) {
                        while (resultSet.next()) {
                            return resultSet.getString(1);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("can not get queryTargetName:" + s + ",sql:" + s2 + " e");
            }
            return null;
        });
    }

    public void init(String sql, String targetName, BiFunction<String, String, String> function) {
        this.sql = Objects.requireNonNull(sql);
        this.queryTargetName = Objects.requireNonNull(targetName);
        this.function = Objects.requireNonNull(function);
    }

    @Override
    public synchronized Number get() {
        if (count > limit) {
            try {
                String s = function.apply(queryTargetName, sql);
                String[] split = SplitUtil.split(s, ',');
                this.count = Long.parseLong(split[0]);
                this.limit = Long.parseLong(split[1]);
            } catch (Throwable e) {
                LOGGER.error("", e);
                throw new RuntimeException(e);
            }
        }
        return (count++);
    }

    @Override
    public void init(SequenceConfig args, long workerId) {
        String[] split = args.getName().split("_");
        String db = split[0];
        String table = split[1];
        init(String.format("select %s.mycat_seq_nextval('%s')",db, args.getName()),"prototype");
    }

    @Override
    public void setStart(Number value) {

    }

}
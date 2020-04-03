package io.mycat.plug.sequence;

import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.SplitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class SequenceMySQLGenerator implements Supplier<String> {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(SequenceMySQLGenerator.class);
    private final String sql;
    private final String queryTargetName;
    private final BiFunction<String, String, String> function;
    private long count = 0;
    private long limit = -1;

    public SequenceMySQLGenerator(String config) {
        this(config, (s, s2) -> {
            String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(s, true, null);
            JdbcDataSource jdbcDataSource = JdbcRuntime.INSTANCE.getConnectionManager().getDatasourceInfo().get(datasourceName);
            try(Connection connection1 = jdbcDataSource.getDataSource().getConnection()){
                try(Statement statement = connection1.createStatement()){
                   try( ResultSet resultSet = statement.executeQuery(s2)) {
                       while (resultSet.next()) {
                           return resultSet.getString(1);
                       }
                   }
                }
            } catch (SQLException e) {
               throw new RuntimeException("can not get queryTargetName:"+s+",sql:"+s2+" e");
            }
            return null;
        });
    }

    public SequenceMySQLGenerator(String config, BiFunction<String, String, String> function) {
        String[] split = config.split(",");
        String queryTargetName = null;
        String sql = null;
        for (String s : split) {
            String[] split1 = s.split(":");
            String key = split1[0].trim().toLowerCase();
            String value = split1[1].trim();
            switch (key) {
                case "sql": {
                    sql = value;
                    break;
                }
                case "targetname": {
                    queryTargetName = value;
                    break;
                }
                default:
                    throw new IllegalArgumentException(key);
            }
        }

        this.sql = Objects.requireNonNull(sql);
        this.queryTargetName = Objects.requireNonNull(queryTargetName);
        this.function = Objects.requireNonNull(function);
    }

    @Override
    public synchronized String get() {
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
        return String.valueOf(count++);
    }
}
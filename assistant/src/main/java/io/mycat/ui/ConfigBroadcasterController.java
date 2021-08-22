package io.mycat.ui;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.config.MycatRouterConfig;
import io.mycat.sqlhandler.ShardingSQLHandler;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigBroadcasterController {
    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigBroadcasterController.class);
    public static String checkConfigConsistency_cmd = "/*+mycat:checkConfigConsistency{}*/";
    public static String syncConfigFromFileToDb_cmd = "/*+mycat:syncConfigFromFileToDb{}*/";
    public static String syncConfigFromDbToFile_cmd = "/*+mycat:syncConfigFromDbToFile{}*/";


    @SneakyThrows
    public static void sync(ConfigBroadcaster broadcaster) {
        ConfigBroadcaster.LoginInfo master = broadcaster.master;
        final DruidDataSource masterDatasource = getDruidDataSource(master);
        ConfigBroadcaster.Type type = broadcaster.type;
        String masterCmd = null;
        switch (type) {
            case DB: {
                masterCmd = syncConfigFromDbToFile_cmd;
                break;
            }
            case FILE: {
                masterCmd = syncConfigFromFileToDb_cmd;
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        JdbcUtils.execute(masterDatasource, masterCmd, Collections.emptyList());
        LOGGER.info("sync master successfully :"+master);
        for (ConfigBroadcaster.LoginInfo slave : broadcaster.slaves) {
            DruidDataSource slaveDatasource = getDruidDataSource(slave);
            JdbcUtils.execute(slaveDatasource, syncConfigFromDbToFile_cmd, Collections.emptyList());
            LOGGER.info("sync slave successfully :"+slave);
        }
        return;

    }

    @NotNull
    private static DruidDataSource getDruidDataSource(ConfigBroadcaster.LoginInfo master) {
        final DruidDataSource druidDataSource = new DruidDataSource();
        String url = master.getUrl();
        String user = master.getUser();
        String password = master.getPassword();

        druidDataSource.setUrl(url);
        druidDataSource.setUsername(user);
        druidDataSource.setPassword(password);
        druidDataSource.setTimeBetweenConnectErrorMillis(TimeUnit.SECONDS.toMillis(3));
        return druidDataSource;
    }
}

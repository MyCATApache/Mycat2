package io.mycat.ui;

import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.config.MycatRouterConfig;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfigBroadcasterController {

    public static  void sync(ConfigBroadcaster broadcaster){
        ConfigBroadcaster.LoginInfo master = broadcaster.master;
        final DruidDataSource masterDatasource = getDruidDataSource(master);
        ConfigBroadcaster.Type type = broadcaster.type;
        switch (type) {
            case DB:
                break;
            case FILE:
                break;
        }
        for (ConfigBroadcaster.LoginInfo slave : broadcaster.slaves) {
            DruidDataSource slaveDatasource = getDruidDataSource(slave);
        }

        System.out.println(masterDatasource);
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

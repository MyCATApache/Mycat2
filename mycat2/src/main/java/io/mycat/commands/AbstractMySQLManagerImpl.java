package io.mycat.commands;

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.ScheduleUtil;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public  abstract class AbstractMySQLManagerImpl implements MySQLManager {

    @Override
    public void setTimer(long delay, Runnable handler) {
        ScheduleUtil.getTimer().schedule(() -> {
            handler.run();
            return null;
        }, delay, TimeUnit.MILLISECONDS);
    }
}

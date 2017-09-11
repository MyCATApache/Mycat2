package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 本地负载情况检查器,此类用于实现本地的负载情况检查
 *
 * Created by ynfeng on 2017/9/12.
 */
public class LocalLoadChecker implements LoadChecker {
    @Override
    public boolean isOverLoad(AdminSession adminSession) {
        return false;
    }

    @Override
    public float getLoadFactor(AdminSession adminSession) {
        return 0;
    }
}

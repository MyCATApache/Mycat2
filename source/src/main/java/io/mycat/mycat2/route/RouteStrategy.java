package io.mycat.mycat2.route;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;

/**
 * 路由策略接口
 *
 */
public interface RouteStrategy {
    public RouteResultset route(SchemaBean schema, byte sqlType, String origSQL, String charset,
            MycatSession mycatSession);
}

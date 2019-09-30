package io.mycat.lib.impl;

import cn.lightfish.pattern.DynamicSQLMatcher;
import io.mycat.proxy.session.MycatSession;

public interface Response {
    void apply(MycatSession session, DynamicSQLMatcher matcher);
}
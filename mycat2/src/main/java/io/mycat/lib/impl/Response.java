package io.mycat.lib.impl;

import io.mycat.pattern.DynamicSQLMatcher;
import io.mycat.proxy.session.MycatSession;

public interface Response {
    void apply(MycatSession session, DynamicSQLMatcher matcher);
}
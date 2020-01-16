package io.mycat.lib.impl;

import io.mycat.proxy.session.MycatSession;

public interface Response {
    void apply(MycatSession session);
}
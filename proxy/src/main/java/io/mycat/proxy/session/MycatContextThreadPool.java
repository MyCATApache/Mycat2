package io.mycat.proxy.session;

import io.mycat.MycatDataContext;
import io.mycat.bindthread.BindThreadCallback;

public interface MycatContextThreadPool {

    void run(MycatDataContext container, BindThreadCallback bindThreadCallback);

    void runOnBinding(MycatDataContext container, BindThreadCallback bindThreadCallback);
}
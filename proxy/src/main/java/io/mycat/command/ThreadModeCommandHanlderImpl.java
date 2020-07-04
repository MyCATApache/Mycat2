package io.mycat.command;

import io.mycat.MycatDataContext;
import io.mycat.proxy.session.MycatSession;
import io.mycat.runtime.ProxySwitch;

import java.util.Objects;

public class ThreadModeCommandHanlderImpl extends ThreadModeCommandDispatcher {
    public ThreadModeCommandHanlderImpl(CommandDispatcher dispatcher) {
        super(dispatcher);
    }


    @Override
    protected void run(MycatSession session, Runnable runnable) {
        ProxySwitch.INSTANCE.stopIfNeed();
        MycatDataContext mycatDataContext = Objects.requireNonNull(session.unwrap(MycatDataContext.class), "Illegal configuration");
        mycatDataContext.run(runnable);
    }
}
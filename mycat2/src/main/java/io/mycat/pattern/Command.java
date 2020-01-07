package io.mycat.pattern;

import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.proxy.session.MycatSession;


public interface Command {
    Runnable apply(MycatClient client, Context context, MycatSession session);

    Runnable explain(MycatClient client, Context context, MycatSession session);
}
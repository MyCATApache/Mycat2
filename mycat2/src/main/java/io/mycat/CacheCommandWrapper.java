package io.mycat;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.boost.BoostRuntime;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.proxy.session.MycatSession;

public class CacheCommandWrapper implements Command {
    final Command command;

    public CacheCommandWrapper(Command command) {
        this.command = command;
    }

    @Override
    public Runnable apply(MycatClient client, Context context, MycatSession session) {
        if (context.isCache()) {
            MycatResultSetResponse response = BoostRuntime.INSTANCE.getResultSetBySqlId(context.getSqlId());
            if (response != null) {
                return () -> SQLExecuterWriter.writeToMycatSession(session, new MycatResponse[]{response});
            }
        }
        return command.apply(client, context, session);

    }

    @Override
    public Runnable explain(MycatClient client, Context context, MycatSession session) {
        return command.apply(client, context, session);
    }
}
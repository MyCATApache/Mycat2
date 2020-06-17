package io.mycat.manager;

import com.google.common.collect.ImmutableList;
import io.mycat.DefaultCommandHandler;
import io.mycat.ReceiverImpl;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.manager.commands.ShowDatasourceCommand;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class ManagerCommandDispatcher extends DefaultCommandHandler {
    static final ImmutableList<MycatCommand> COMMANDS = ImmutableList.of(
            new ShowDatasourceCommand()
    );

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {

        ////////////////////////////////////////////////////////////////////////////////
        String original = new String(bytes);
        original = original.trim();
        original = original.endsWith(";") ? original.substring(0, original.length() - 1) : original;

        /////////////////////////////////////////////////////////////////////////////////
        MycatRequest mycatRequest = new MycatRequest(session.sessionId(), original, new HashMap<>(), null);

        ReceiverImpl receiver = new ReceiverImpl(session);

        for (MycatCommand command : COMMANDS) {
            if (command.run(mycatRequest, session.getDataContext(), receiver)) {
                return;
            }
        }
        super.handleQuery(bytes, session);
    }
}
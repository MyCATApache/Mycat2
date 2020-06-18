package io.mycat.manager.commands;

import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.MycatUser;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.ReactorThreadManager;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager;
import io.mycat.util.Response;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ShowConnectionCommand implements MycatCommand {
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if ("show @@connection".equalsIgnoreCase(request.getText())){
            ReactorThreadManager reactorManager = MycatCore.INSTANCE.getReactorManager();
            Objects.requireNonNull(reactorManager);
            List<MycatSession> sessions = reactorManager.getList().stream().flatMap(i -> i.getFrontManager().getAllSessions().stream()).collect(Collectors.toList());
            for (MycatSession session : sessions) {
                MycatUser user = session.getUser();
                String userName = user.getUserName();
                String host = user.getHost();
                byte[] password = user.getPassword();
            }
            response.sendOk();
        }
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        return false;
    }

    @Override
    public String getName() {
        return getClass().getName();
    }
}
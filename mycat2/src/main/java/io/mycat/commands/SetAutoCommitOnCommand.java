package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum SetAutoCommitOnCommand implements MycatCommand {
    INSTANCE;
    final static Logger LOGGER = LoggerFactory.getLogger(SetAutoCommitOnCommand.class);
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        context.setAutoCommit(true);
        LOGGER.debug("session id:{} action:set autocommit = 1 exe success", request.getSessionId());
        response.sendOk();
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        response.sendExplain(SetAutoCommitOnCommand.class,"SET_AUTOCOMMIT_ON");
        return true;
    }

    @Override
    public String getName() {
        return "setAutoCommitOn";
    }
}
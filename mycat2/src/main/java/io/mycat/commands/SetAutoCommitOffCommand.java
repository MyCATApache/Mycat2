package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetAutoCommitOffCommand implements MycatCommand {
    final static Logger LOGGER = LoggerFactory.getLogger(SetAutoCommitOffCommand.class);
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        context.setAutoCommit(false);
        LOGGER.debug("session id:{} action:set autocommit = 0 exe success", request.getSessionId());
        response.sendOk();
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        response.sendExplain(SetAutoCommitOffCommand.class,"SET_AUTOCOMMIT_OFF");
        return true;
    }

    @Override
    public String getName() {
        return "setAutoCommitOff";
    }
}
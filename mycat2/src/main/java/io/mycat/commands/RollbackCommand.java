package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollbackCommand implements MycatCommand {

   private static final Logger LOGGER = LoggerFactory.getLogger(RollbackCommand.class);

    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        response.rollback();
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        response.sendExplain(RollbackCommand.class,"rollback");
        return true;
    }

    @Override
    public String getName() {
        return "rollback";
    }
}
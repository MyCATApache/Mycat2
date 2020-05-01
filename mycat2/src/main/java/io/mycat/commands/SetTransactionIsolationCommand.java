package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class SetTransactionIsolationCommand implements MycatCommand{
    final static Logger LOGGER = LoggerFactory.getLogger(SetAutoCommitOffCommand.class);
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        MySQLIsolation mySQLIsolation = MySQLIsolation.parse(Objects.requireNonNull(request.get("transactionIsolation")));
        context.setIsolation(mySQLIsolation);
        LOGGER.debug("session id:{} action: set isolation = {}", request.getSessionId(), mySQLIsolation);
        response.sendOk();
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        MySQLIsolation mySQLIsolation = MySQLIsolation.parse(Objects.requireNonNull(request.get("transactionIsolation")));
        response.sendExplain(SetTransactionIsolationCommand.class,mySQLIsolation);
        return true;
    }

    @Override
    public String getName() {
        return "setTransactionIsolation";
    }
}
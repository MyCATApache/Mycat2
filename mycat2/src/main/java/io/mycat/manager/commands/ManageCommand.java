package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.util.Response;

public interface ManageCommand extends MycatCommand {
    String statement();

    String description();

    void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception;

    @Override
    default boolean run(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        if (statement().equalsIgnoreCase(request.getText())) {
            handle(request, context, response);
            return true;
        }
        return false;
    }

    @Override
    default String getName() {
        return getClass().getName();
    }
}
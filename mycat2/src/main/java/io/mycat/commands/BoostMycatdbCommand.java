package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.booster.BoosterRuntime;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

import java.util.Optional;

public enum BoostMycatdbCommand implements MycatCommand {
    INSTANCE;

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if (!context.isInTransaction() || context.isAutocommit()) {
            Optional<String> booster = BoosterRuntime.INSTANCE.getBooster(context.getUser().getUserName());
            if (booster.isPresent()) {
                response.proxySelect(booster.get(), request.getText());
                return true;
            }
        }
        return MycatdbCommand.INSTANCE.run(request, context, response);
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        response.sendExplain(BoostMycatdbCommand.class, "boostMycatdb");
        return true;
    }

    @Override
    public String getName() {
        return "boostMycatdb";
    }
}
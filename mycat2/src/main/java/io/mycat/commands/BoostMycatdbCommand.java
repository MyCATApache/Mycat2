package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.booster.BoosterRuntime;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public enum BoostMycatdbCommand implements MycatCommand {
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(BoostMycatdbCommand.class);

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if (!context.isInTransaction() && context.isAutocommit()) {
            try {
                Object boostersInfo = request.get("boosters");
                String booster = null;
                if (boostersInfo != null) {
                    if (boostersInfo instanceof List) {
                        List<String> b = (List) boostersInfo;
                        b = b.stream().filter(i -> i != null).collect(Collectors.toList());
                        int randomIndex = ThreadLocalRandom.current().nextInt(0, b.size());
                        booster = b.get(randomIndex);
                    } else if (boostersInfo instanceof String) {
                        booster = (String) boostersInfo;
                    } else {
                        logger.error(request.toString());
                    }
                }
                if (booster == null) {
                    booster = BoosterRuntime.INSTANCE.getBooster(context.getUser().getUserName()).orElse(null);
                }
                if (booster != null) {
                    response.proxySelect(booster, request.getText());
                    return true;
                }
            }catch (Throwable e){
                logger.error("",e);
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
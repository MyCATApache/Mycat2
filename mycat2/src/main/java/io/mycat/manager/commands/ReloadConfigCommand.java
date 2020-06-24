package io.mycat.manager.commands;

import io.mycat.MycatConfig;
import io.mycat.MycatDataContext;
import io.mycat.MycatWorkerProcessor;
import io.mycat.RootHelper;
import io.mycat.booster.BoosterRuntime;
import io.mycat.client.InterceptorRuntime;
import io.mycat.client.MycatRequest;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.metadata.MetadataManager;
import io.mycat.plug.PlugRuntime;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;


/**
 * 暂不可用
 */
public class ReloadConfigCommand implements ManageCommand {
    @Override
    public String statement() {
        return "reload @@config";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        try {
            RootHelper.INSTANCE.getConfigProvider().fetchConfig();
            MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();


            PlugRuntime.INSTANCE.load(mycatConfig);
            MycatWorkerProcessor.INSTANCE.init(mycatConfig.getServer().getWorkerPool(),mycatConfig.getServer().getTimeWorkerPool());
            ReplicaSelectorRuntime.INSTANCE.load(mycatConfig);
            JdbcRuntime.INSTANCE.load(mycatConfig);
            BoosterRuntime.INSTANCE.load(mycatConfig);
            InterceptorRuntime.INSTANCE.load(mycatConfig);


            MetadataManager.INSTANCE.load(mycatConfig);
            response.sendOk();
        } catch (Exception e) {
            e.printStackTrace();
            response.sendError(e);
        }

    }
}
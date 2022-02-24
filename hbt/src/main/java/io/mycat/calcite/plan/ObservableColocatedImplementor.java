package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.DrdsSqlWithParams;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.calcite.spm.Plan;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ObservableColocatedImplementor extends ObservablePlanImplementorImpl {
    protected final static Logger LOGGER = LoggerFactory.getLogger(ObservableColocatedImplementor.class);
    public ObservableColocatedImplementor(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        super(xaSqlConnection, context, drdsSqlWithParams, response);
    }


    @Override
    public Future<Void> executeQuery(Plan plan) {
        Optional<ExplainDetail> singleViewOptional = ColocatedPlanner.executeQuery(this.context, plan, this.drdsSqlWithParams);
        if (singleViewOptional.isPresent()) {
            ExplainDetail explainDetail = singleViewOptional.get();
            return response.proxySelect(explainDetail.getTargets(), explainDetail.getSql(), explainDetail.getParams());
        }
        return super.executeQuery(plan);
    }
}

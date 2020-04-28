package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.PlanRunner;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;
import io.mycat.client.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class DistributedQueryCommand implements MycatCommand{
    final static Logger logger = LoggerFactory.getLogger(DistributedQueryCommand.class);
    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        String sql = request.getText();
        MycatDBClientMediator client = MycatDBs.createClient(context);
        MycatSQLPrepareObject mycatSQLPrepareObject = client.getUponDBSharedServer().innerQueryPrepareObject(client.sqlContext().simplySql(sql), client);
        PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
        response.sendResultSet(plan.run());
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
        String sql = request.getText();
        MycatDBClientMediator client = MycatDBs.createClient(context);
        MycatSQLPrepareObject mycatSQLPrepareObject = client.getUponDBSharedServer().innerQueryPrepareObject(client.sqlContext().simplySql(sql), client);
        PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
        response.sendExplain(DistributedQueryCommand.class,plan);
        return true;
    }

    @Override
    public String getName() {
        return "distributedQuery";
    }
}
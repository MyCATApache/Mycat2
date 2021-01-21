package io.mycat.sqlhandler;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.*;
import io.mycat.calcite.executor.Group;
import io.mycat.calcite.executor.MycatInsertExecutor;
import io.mycat.calcite.executor.MycatUpdateExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.util.Pair;
import io.mycat.util.SQL;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        HackRouter hackRouter = new HackRouter(request.getAst(), dataContext);
        if (false) {
            Pair<String, String> plan = hackRouter.getPlan();
            response.proxySelect(plan.getKey(), plan.getValue());
        } else {
            DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
            drdsRunner.runOnDrds(dataContext, request.getAst(), response);
        }
    }

}
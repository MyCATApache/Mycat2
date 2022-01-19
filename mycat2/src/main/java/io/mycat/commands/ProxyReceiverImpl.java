package io.mycat.commands;

import cn.mycat.vertx.xa.MySQLManager;
import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.NewMycatConnectionConfig;
import io.mycat.proxy.session.MySQLServerSession;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

public class ProxyReceiverImpl extends ReceiverImpl {
    public ProxyReceiverImpl(MySQLServerSession session, int stmtSize, boolean binary) {
        super(session, stmtSize, binary);
    }

    @Override
    public Future<Void> execute(ExplainDetail detail) {
        MycatDataContext dataContext = session.getDataContext();
        if (false&&count == 0 && !binary && !dataContext.isInTransaction() &&
                (detail.getExecuteType() == ExecuteType.QUERY || detail.getExecuteType() == ExecuteType.QUERY_MASTER)
                && detail.getTargets().size() == 1
                && MySQLServerCapabilityFlags.isDeprecateEOF(dataContext.getServerCapabilities()) == NewMycatConnectionConfig.CLIENT_DEPRECATE_EOF) {
            String targetName = dataContext.resolveDatasourceTargetName(detail.getTargets().get(0));
            MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
            Future<NewMycatConnection> connection = mySQLManager.getConnection(targetName);
            return connection.flatMap(connection1 -> {
                Observable<Buffer> bufferObservable = connection1.prepareQuery(detail.getSql(), detail.getParams(),session.getServerStatusValue());
                bufferObservable= bufferObservable.doOnComplete(() -> connection1.close());
                bufferObservable=  bufferObservable;
                return swapBuffer(bufferObservable);
            });
        }
        return super.execute(detail);
    }
}

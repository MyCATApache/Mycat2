package io.mycat.commands;

import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.runtime.MycatXaTranscation;
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
        if (count == 1 && !binary && !dataContext.isInTransaction() &&
                (detail.getExecuteType() == ExecuteType.QUERY || detail.getExecuteType() == ExecuteType.QUERY_MASTER)
                && detail.getTargets().size() == 1) {
            String targetName = dataContext.resolveDatasourceTargetName(detail.getTargets().get(0));
            MycatXaTranscation transactionSession = (MycatXaTranscation) dataContext.getTransactionSession();
            Future<NewMycatConnection> connection = transactionSession.getConnection(targetName);
            return connection.flatMap(connection1 -> {
                Observable<Buffer> bufferObservable = connection1.prepareQuery(detail.getSql(), detail.getParams());
                return swapBuffer(bufferObservable).eventually(unused -> transactionSession.closeStatementState());
            });
        }
        return super.execute(detail);
    }
}

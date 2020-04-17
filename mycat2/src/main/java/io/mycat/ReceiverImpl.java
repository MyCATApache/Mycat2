package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.hbt.TextUpdateInfo;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.upondb.PlanRunner;
import io.mycat.util.Receiver;
import lombok.AllArgsConstructor;

import java.util.Iterator;

@AllArgsConstructor
public class ReceiverImpl implements Receiver {
    MycatSession session;
    MycatClient client;
    Context context;
    @Override
    public void setHasMore(boolean more) {
        if (more){
            session.setLastMessage("unsupport multi statements");
            session.writeErrorEndPacketBySyncInProcessError();
        }

    }

    @Override
    public void sendError(Throwable e) {
        session.setLastMessage(e);
        session.writeErrorEndPacketBySyncInProcessError();

    }

    @Override
    public void sendOk() {
        session.writeOkEndPacket();
    }


    @Override
    public void evalSimpleSql(SQLSelectStatement toString) {
        //没有处理的sql,例如没有替换事务状态,自动提交状态的sql,随机发到后端会返回该随机的服务器状态
        if (session.isBindMySQLSession()) {
            MySQLTaskUtil.proxyBackend(session, toString.toString());
        } else {
            String datasourceNameByRandom = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
            MySQLTaskUtil.proxyBackendByTargetName(session, datasourceNameByRandom, toString.toString(),
                    MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                    session.getIsolation(), false, null);
        }
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        context.putVaribale("targets",defaultTargetName);
        context.putVaribale("metaData","false");
        context.putVaribale("executeType",ContextRunner.ExecuteType.QUERY.name());
        context.putVaribale("needTransaction","true");
        Command command1 = ContextRunner.COMMANDS.get(ContextRunner.EXECUTE);
        command1.apply(client,context,session).run();
    }

    @Override
    public void eval(PlanRunner plan) {
        try {
            TextResultSetResponse connection = new TextResultSetResponse(plan.run());
            SQLExecuterWriter.writeToMycatSession(session, new MycatResponse[]{connection});
        }finally {
            client.getMycatDb().recycleResource();//移除已经关闭的连接,
        }
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String sql) {
        context.setSql(sql);
        context.putVaribale("targets",defaultTargetName);
        context.putVaribale("metaData","false");
        context.putVaribale("executeType",ContextRunner.ExecuteType.UPDATE.name());
        context.putVaribale("needTransaction","true");
        Command command1 = ContextRunner.COMMANDS.get(ContextRunner.EXECUTE);
        command1.apply(client,context,session).run();
    }



    @Override
    public void proxyDDL(SQLStatement statement) {
        if (session.isBindMySQLSession()) {
            MySQLTaskUtil.proxyBackend(session, statement.toString());
        } else {
            String datasourceNameByRandom = ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
            MySQLTaskUtil.proxyBackendByTargetName(session, datasourceNameByRandom, statement.toString(),
                    MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                    session.getIsolation(), false, null);
        }
    }

    @Override
    public void proxyShow(SQLStatement statement) {
        proxyDDL(statement);
    }

    @Override
    public void multiUpdate(String string, Iterator<TextUpdateInfo> apply) {
        ContextRunner.executeSupplier.apply(ContextRunner.ExecuteType.UPDATE).apply(client,context,session);
    }

    @Override
    public void multiInsert(String string, Iterator<TextUpdateInfo> apply) {
        ContextRunner.executeSupplier.apply(ContextRunner.ExecuteType.INSERT).apply(client,context,session);

    }
}
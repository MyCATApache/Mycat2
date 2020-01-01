package io.mycat;

import io.mycat.client.ClientRuntime;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.pattern.ContextRunner;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatUser;

public class DefaultCommandHandler extends AbstractCommandHandler {
    private MycatClient client;

    @Override
    public void initRuntime(MycatSession session) {
        MycatUser user = session.getUser();
        this.client = ClientRuntime.INSTANCE.login(user.getUserName(), user.getPassword());
        this.client.useSchema(session.getSchema());
        this.client.useTransactionType(ContextRunner.PROXY_TRANSACTION_TYPE);
    }

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        System.out.println("-----------------reveice--------------------");
        String sql = new String(bytes);
        System.out.println(sql);
        Context analysis = client.analysis(sql);
        ContextRunner.run(client,analysis,session);
//        session.writeOkEndPacket();
    }

    @Override
    public void handleContentOfFilename(byte[] sql, MycatSession session) {

    }

    @Override
    public void handleContentOfFilenameEmptyOk(MycatSession session) {
        session.writeOkEndPacket();
    }

    @Override
    public void handlePrepareStatement(byte[] sql, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int numParams, byte[] rest, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementClose(long statementId, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {

    }

    @Override
    public void handlePrepareStatementReset(long statementId, MycatSession session) {

    }

    @Override
    public int getNumParamsByStatementId(long statementId, MycatSession session) {
        return 0;
    }
}
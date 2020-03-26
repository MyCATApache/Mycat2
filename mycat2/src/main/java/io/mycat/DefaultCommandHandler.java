/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import io.mycat.client.ClientRuntime;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {
    private MycatClient client;
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(DefaultCommandHandler.class);

    @Override
    public void handleInitDb(String db, MycatSession mycat) {
        client.useSchema(db);
        mycat.setSchema(db);
        LOGGER.info("handleInitDb:" + db);
        super.handleInitDb(db, mycat);
    }

    @Override
    public void initRuntime(MycatSession session) {
        this.client = ClientRuntime.INSTANCE.login((MycatDataContext) session.unwrap(MycatDataContext.class), true);
        this.client.useSchema(session.getSchema());
    }

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        try {
            LOGGER.debug("-----------------reveice--------------------");
            String sql = new String(bytes);
            LOGGER.debug(sql);
            sql = sql.trim();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
                LOGGER.debug("-----------------tirm-right-semi(;)--------------------");
            }
            MycatDataContext unwrap = session.unwrap(MycatDataContext.class);
            TransactionSession transactionSession = unwrap.getTransactionSession();
            Context analysis = client.analysis(sql);
            ContextRunner.run(client, analysis, session);
        } catch (Exception e) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        }
    }

    @Override
    public void handleContentOfFilename(byte[] sql, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handleContentOfFilenameEmptyOk(MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatement(byte[] sql, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int numParams, byte[] rest, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementClose(long statementId, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementReset(long statementId, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public int getNumParamsByStatementId(long statementId, MycatSession session) {
        return 0;
    }
}
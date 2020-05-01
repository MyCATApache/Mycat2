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


import io.mycat.client.Interceptor;
import io.mycat.client.InterceptorRuntime;
import io.mycat.client.UserSpace;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;

import java.nio.ByteBuffer;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

  //  private MycatClient client;
  //  private final ApplicationContext applicationContext = MycatCore.INSTANCE.getContext();
  //  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(DefaultCommandHandler.class);
  //  private final Set<SQLHandler> sqlHandlers = new TreeSet<>(new OrderComparator(Arrays.asList(Order.class)));
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(DefaultCommandHandler.class);
    private Interceptor interceptor;


    @Override
    public void handleInitDb(String db, MycatSession mycat) {
        mycat.useSchema(db);
        LOGGER.info("handleInitDb:" + db);
        super.handleInitDb(db, mycat);
    }

    @Override
    public void initRuntime(MycatSession session) {

   //     this.client = ClientRuntime.INSTANCE.login((MycatDataContext) session.unwrap(MycatDataContext.class), true);
   //     this.client.useSchema(session.getSchema());
   //     this.sqlHandlers.addAll(applicationContext.getBeanForType(SQLHandler.class));

        this.interceptor = InterceptorRuntime.INSTANCE.login(session.getUser().getUserName());
    }

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        try {
// 1.02-future-metadata-2020-4-12
     //       LOGGER.debug("-----------------reveice--------------------");
     //       String sql = new String(bytes);
    //        LOGGER.debug(sql);
    //        sql = sql.trim();
    //        if (sql.endsWith(";")) {
    //            sql = sql.substring(0, sql.length() - 1);
   ////             LOGGER.debug("-----------------tirm-right-semi(;)--------------------");
  //          }
  //          Context analysis = client.analysis(sql);
//            SQLHanlder sqlHanlder = new SQLHanlder(client.getMycatDb().sqlContext());
//            ReceiverImpl receiver = new ReceiverImpl(session, client, analysis);
//            sqlHanlder.parse(sql, receiver);
  //          executeQuery(sql,new ReceiverImpl(session, client, analysis),client.getMycatDb().sqlContext(),session);

//            ContextRunner.run(client, analysis, session);

            UserSpace userSpace = this.interceptor.getUserSpace();
            userSpace.execute(ByteBuffer.wrap(bytes), session,new ReceiverImpl(session));
        } catch (Throwable e) {
            LOGGER.debug("-----------------reveice--------------------");
            LOGGER.debug(new String(bytes));
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
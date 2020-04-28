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
=======
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
      //  try {
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

    private void executeQuery(String sql, Receiver receiver, SQLContext context, MycatSession session){
        int totalSqlMaxCode = 0;
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, false);
        LinkedList<SQLStatement> statementList = new LinkedList<SQLStatement>();
        parser.parseStatementList(statementList, -1, null);

        Iterator<SQLStatement> iterator = statementList.iterator();
        while (iterator.hasNext()) {
            SQLStatement statement = iterator.next();
            statement.accept(new ContextExecuter(context));
            receiver.setHasMore(iterator.hasNext());
            SQLHandler.SQLRequest<SQLStatement> request = new SQLHandler.SQLRequest<>(statement,context);
            try {
                int code = 0;
                int executeCount = 0;
                for (SQLHandler sqlHandler : sqlHandlers) {
                    int returnCode = sqlHandler.execute(request,receiver,session);
                    code |= returnCode;
                    if(code != SQLHandler.CODE_0){
                        executeCount++;
                    }
                }
                totalSqlMaxCode |= code;
                if(code == SQLHandler.CODE_0){
                    //程序未执行
                }else if(code < SQLHandler.CODE_100){
                    //1到99区间, 预留系统内部状态
                }else if(code < SQLHandler.CODE_200){
                    //100到199区间, 未执行完,等待下次请求继续执行
                }else if(code < SQLHandler.CODE_300){
                    //200到299区间, 执行正常
                }else if(code < SQLHandler.CODE_400){
                    //300到399区间, 代理错误
                }else if(code < SQLHandler.CODE_500){
                    //400到499区间,客户端错误
                }else {
                    //500以上, 服务端错误
                }
            } catch (Throwable e) {
                receiver.sendError(e);
                return;
            } finally {
                iterator.remove();//help gc
            }
        }

        if(totalSqlMaxCode == SQLHandler.CODE_0){
            //程序未执行
            receiver.sendError(new MycatException("no support query. sql={}",sql));
        }if(totalSqlMaxCode < SQLHandler.CODE_100){
            //1到99区间, 预留系统内部状态
        }else if(totalSqlMaxCode < SQLHandler.CODE_200){
            //100到199区间, 未执行完,等待下次请求继续执行
        }else if(totalSqlMaxCode < SQLHandler.CODE_300){
            //200到299区间, 执行正常
        }else if(totalSqlMaxCode < SQLHandler.CODE_400){
            //300到399区间, 代理错误
        }else if(totalSqlMaxCode < SQLHandler.CODE_500){
            //400到499区间,客户端错误
        }else {
            //500以上, 服务端错误
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
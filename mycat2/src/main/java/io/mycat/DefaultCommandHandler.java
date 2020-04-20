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

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import io.mycat.client.ClientRuntime;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.util.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {
    private MycatClient client;
    private final ApplicationContext applicationContext = MycatCore.INSTANCE.getContext();
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
            Context analysis = client.analysis(sql);
//            SQLHanlder sqlHanlder = new SQLHanlder(client.getMycatDb().sqlContext());
//            ReceiverImpl receiver = new ReceiverImpl(session, client, analysis);
//            sqlHanlder.parse(sql, receiver);
            executeQuery(sql,new ReceiverImpl(session, client, analysis),client.getMycatDb().sqlContext(),session);

//            ContextRunner.run(client, analysis, session);
        } catch (Throwable e) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        }
    }

    private void executeQuery(String sql, Receiver receiver, SQLContext context, MycatSession session){
        int totalSqlMaxCode = 0;
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, false);
        LinkedList<SQLStatement> statementList = new LinkedList<SQLStatement>();
        parser.parseStatementList(statementList, -1, null);

        List<SQLHandler> sqlHandlers = applicationContext.getBeanForType(SQLHandler.class);
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
                }if(code < SQLHandler.CODE_100){
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
            receiver.sendError(new MycatException("no support query. sql={0}",sql));
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
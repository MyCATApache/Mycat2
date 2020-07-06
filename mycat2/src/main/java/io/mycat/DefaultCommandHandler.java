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


import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.client.InterceptorRuntime;
import io.mycat.client.UserSpace;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.metadata.MetadataManager;
import io.mycat.preparestatement.PrepareStatementManager;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.session.MycatSession;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

    //  private MycatClient client;
    //  private final ApplicationContext applicationContext = MycatCore.INSTANCE.getContext();
    //  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(DefaultCommandHandler.class);
    //  private final Set<SQLHandler> sqlHandlers = new TreeSet<>(new OrderComparator(Arrays.asList(Order.class)));
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommandHandler.class);
    private static boolean useServerPrepStmts = Optional.ofNullable(RootHelper.INSTANCE.getConfigProvider().currentConfig())
            .map(i -> i.getProperties()).map(i -> i.get("useServerPrepStmts")).isPresent();

    @Override
    public void handleInitDb(String db, MycatSession mycat) {
        mycat.useSchema(db);
        LOGGER.info("handleInitDb:" + db);
        super.handleInitDb(db, mycat);
    }

    @Override
    public void initRuntime(MycatSession session) {
        UserSpace interceptor = InterceptorRuntime.INSTANCE.getUserSpace(session.getUser().getUserName());
        TransactionType defaultTransactionType = interceptor.getDefaultTransactionType();
        if (defaultTransactionType != null) {
            session.getDataContext().switchTransaction(defaultTransactionType);
        }
    }

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("-----------------reveice--------------------");
                LOGGER.debug(new String(bytes));
            }
            UserSpace userSpace = InterceptorRuntime.INSTANCE.getUserSpace(session.getUser().getUserName());
            userSpace.execute(ByteBuffer.wrap(bytes), session, new ReceiverImpl(session));
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
    public void handlePrepareStatement(byte[] sqlBytes, MycatSession session) {
        if (!useServerPrepStmts) {
            ReceiverImpl receiver = new ReceiverImpl(session);
            receiver.sendError(new MycatException("unsupported useServerPrepStmts"));
            return;
        }
        MycatDataContext dataContext = session.getDataContext();
        boolean deprecateEOF = session.isDeprecateEOF();
        String sql = new String(sqlBytes);
        /////////////////////////////////////////////////////

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        ResultSetBuilder fieldsBuilder = ResultSetBuilder.create();
        if (sqlStatement instanceof SQLSelectStatement) {
            List<SQLSelectItem> selectList = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock().getSelectList();
            for (SQLSelectItem sqlSelectItem : selectList) {
                SQLDataType sqlDataType = sqlSelectItem.computeDataType();
                JDBCType res = JDBCType.VARCHAR;
                if (sqlDataType != null) {
                    res = JDBCType.valueOf(sqlDataType.jdbcType());
                }
                if (res == null) {
                    res = JDBCType.VARCHAR;
                }
                fieldsBuilder.addColumnInfo(sqlSelectItem.toString(), res);
            }
        }
        MycatRowMetaData fields = fieldsBuilder.build().getMetaData();
        ResultSetBuilder paramsBuilder = ResultSetBuilder.create();

        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    SQLDataType sqlDataType = x.computeDataType();
                    JDBCType res = JDBCType.VARCHAR;
                    if (sqlDataType != null) {
                        res = JDBCType.valueOf(sqlDataType.jdbcType());
                    }
                    paramsBuilder.addColumnInfo("", res);
                }
                super.endVisit(x);
            }
        });

        MycatRowMetaData params = paramsBuilder.build().getMetaData();
        long stmtId = PrepareStatementManager.INSTANCE.register(sql, params.getColumnCount());
        Map<Long, PreparedStatement> statementMap = dataContext.getPrepareInfo();
        statementMap.put(stmtId, new PreparedStatement(stmtId, sqlStatement, params.getColumnCount()));

        DefaultPreparedOKPacket info = new DefaultPreparedOKPacket(stmtId, fields.getColumnCount(), params.getColumnCount(), session.getWarningCount());

        if (info.getPrepareOkColumnsCount() == 0 && info.getPrepareOkParametersCount() == 0) {
            session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), true);
            return;
        }
        session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), false);
        if (info.getPrepareOkParametersCount() > 0 && info.getPrepareOkColumnsCount() == 0) {
            for (int i = 1; i <= info.getPrepareOkParametersCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        } else if (info.getPrepareOkParametersCount() == 0 && info.getPrepareOkColumnsCount() > 0) {
            for (int i = 1; i <= info.getPrepareOkColumnsCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        } else {
            for (int i = 1; i <= info.getPrepareOkParametersCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            session.writeColumnEndPacket();
            for (int i = 1; i <= info.getPrepareOkColumnsCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        }
    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.appendLongData(paramId, data);
        }
        session.onHandlerFinishedClear();
    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int[] params, BindValue[] values, MycatSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        String sql = preparedStatement.getSqlByBindValue(values);
        MycatDBClientMediator client = MycatDBs.createClient(dataContext);
        try {
            if (preparedStatement.isQuery()) {
                RowBaseIterator baseIterator = client.query(sql);
                ReceiverImpl receiver = new ReceiverImpl(session);
                receiver.sendBinaryResultSet(() -> baseIterator);
            } else {
                RowBaseIterator baseIterator = client.query(sql);
                ReceiverImpl receiver = new ReceiverImpl(session);
                receiver.sendResponse(new MycatResponse[]{(UpdateRowIteratorResponse) baseIterator}, null);
            }
        } finally {
            client.close();
        }
    }

    @Override
    public void handlePrepareStatementClose(long statementId, MycatSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        longPreparedStatementMap.remove(statementId);
        session.onHandlerFinishedClear();
    }

    @Override
    public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementReset(long statementId, MycatSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.resetLongData();
        }
        session.writeOkEndPacket();
    }

    @Override
    public int getNumParamsByStatementId(long statementId, MycatSession session) {
        return PrepareStatementManager.INSTANCE.getNum(statementId);
    }

    @Override
    public byte[] getLongData(long statementId, int paramId, MycatSession mycat) {
        PreparedStatement preparedStatement = mycat.getDataContext().getPrepareInfo().get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        ByteArrayOutputStream byteArrayOutputStream = preparedStatement.getLongData(paramId);
        if (byteArrayOutputStream == null) {
            return null;
        }

        byte[] bytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.reset();
        return bytes;
    }

    @Override
    public BindValue[] getLastBindValue(long statementId, MycatSession mycat) {
        Map<Long, PreparedStatement> prepareInfo = mycat.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        return preparedStatement.getBindValues();
    }

    @Override
    public void saveBindValue(long statementId, BindValue[] values, MycatSession mycat) {
        Map<Long, PreparedStatement> prepareInfo = mycat.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return ;
        }
         preparedStatement.setBindValues(values);
    }
}
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
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.commands.MycatdbCommand;
import io.mycat.config.UserConfig;
import io.mycat.proxy.session.MycatSession;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.sql.JDBCType;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {
   final static AtomicLong ids = new AtomicLong(0);
    //  private MycatClient client;
    //  private final ApplicationContext applicationContext = MycatCore.INSTANCE.getContext();
    //  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(DefaultCommandHandler.class);
    //  private final Set<SQLHandler> sqlHandlers = new TreeSet<>(new OrderComparator(Arrays.asList(Order.class)));
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommandHandler.class);

    @Override
    public void handleInitDb(String db, MycatSession mycat) {
        mycat.useSchema(db);
        LOGGER.info("handleInitDb:" + db);
        super.handleInitDb(db, mycat);
    }

    @Override
    public void initRuntime(MycatSession session) {
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        UserConfig userInfo = authenticator.getUserInfo(session.getUser().getUserName());
        if (userInfo != null) {
            session.getDataContext().switchTransaction(TransactionType.parse(userInfo.getTransactionType()));
        }
    }

    @Override
    public void handleQuery(byte[] bytes, MycatSession session) {
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("-----------------reveice--------------------");
                LOGGER.debug(new String(bytes));
            }
            Boolean hasRun = false;
            if (!hasRun) {
                MycatdbCommand.INSTANCE.executeQuery(new String(bytes), session, session.getDataContext());
                return;
            }
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
        try {
            MycatDataContext dataContext = session.getDataContext();
            boolean deprecateEOF = session.isDeprecateEOF();
            String sql = new String(sqlBytes);
            /////////////////////////////////////////////////////

            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            boolean allow =  (sqlStatement instanceof SQLSelectStatement
                    ||
                    sqlStatement instanceof SQLInsertStatement
                    ||
                    sqlStatement instanceof SQLUpdateStatement
                    ||
                    sqlStatement instanceof SQLDeleteStatement
            );
//            if (!allow){
//                session.writeErrorEndPacketBySyncInProcessError();
//                return;
//            }
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            metadataManager.resolveMetadata(sqlStatement);
            ResultSetBuilder fieldsBuilder = ResultSetBuilder.create();
//            if (sqlStatement instanceof SQLSelectStatement) {
//                List<SQLSelectItem> selectList = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock().getSelectList();
//                for (SQLSelectItem sqlSelectItem : selectList) {
//                    SQLDataType sqlDataType = sqlSelectItem.computeDataType();
//                    JDBCType res = JDBCType.VARCHAR;
//                    if (sqlDataType != null) {
//                        res = JDBCType.valueOf(sqlDataType.jdbcType());
//                    }
//                    if (res == null) {
//                        res = JDBCType.VARCHAR;
//                    }
//                    fieldsBuilder.addColumnInfo(sqlSelectItem.toString(), res);
//                }
//            }
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
            long stmtId = ids.getAndIncrement();
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
        }catch (Throwable throwable){
            ReceiverImpl receiver = new ReceiverImpl(session, 1, false, false);
            receiver.sendError(throwable);
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
    @SneakyThrows
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int[] params, BindValue[] values, MycatSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        SQLStatement statement = preparedStatement.getSQLStatementByBindValue(values);
        LOGGER.info("=>"+statement);
        ReceiverImpl receiver = new ReceiverImpl(session, 1, true, false);
        session.getDataContext().block(new Runnable() {
            @Override
            @SneakyThrows
            public void run() {
                MycatdbCommand.execute(dataContext, receiver, statement);
            }
        });
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
        Map<Long, PreparedStatement> prepareInfo = session.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        Objects.requireNonNull(preparedStatement);
        return preparedStatement.getParametersNumber();
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
            return;
        }
        preparedStatement.setBindValues(values);
    }
}
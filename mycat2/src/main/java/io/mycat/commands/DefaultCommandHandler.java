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
package io.mycat.commands;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.config.UserConfig;
import io.mycat.proxy.session.MySQLServerSession;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.sql.JDBCType;
import java.util.Map;
import java.util.Objects;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommandHandler.class);


    @Override
    public void initRuntime(MySQLServerSession session) {

    }

    @Override
    public Future<Void> handleQuery(byte[] bytes, MySQLServerSession session) {
        try {
            String sql = new String(bytes);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("-----------------reveice--------------------");
                LOGGER.debug(sql);
            }
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            return vertx.executeBlocking(event -> {
                Future<Void> promise =
                        MycatdbCommand.INSTANCE.executeQuery(sql, session.getDataContext(),
                                (size) -> new ReceiverImpl(session, size, false));
                promise.onComplete(event);
            });
        } catch (Throwable e) {
            return Future.failedFuture(e);
        }
    }


    @Override
    public Future<Void> handleContentOfFilename(byte[] sql, MySQLServerSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public Future<Void> handleContentOfFilenameEmptyOk(MySQLServerSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public Future<Void> handlePrepareStatement(byte[] sqlBytes, MySQLServerSession session) {
        try {
            MycatDataContext dataContext = session.getDataContext();
            boolean deprecateEOF = session.isDeprecateEOF();
            String sql = new String(sqlBytes);
            /////////////////////////////////////////////////////

            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            boolean allow = (sqlStatement instanceof SQLSelectStatement
                    ||
                    sqlStatement instanceof SQLInsertStatement
                    ||
                    sqlStatement instanceof SQLUpdateStatement
                    ||
                    sqlStatement instanceof SQLDeleteStatement
            );
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            metadataManager.resolveMetadata(sqlStatement);
            ResultSetBuilder fieldsBuilder = ResultSetBuilder.create();
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
            long stmtId = dataContext.nextPrepareStatementId();
            Map<Long, PreparedStatement> statementMap = dataContext.getPrepareInfo();
            statementMap.put(stmtId, new PreparedStatement(stmtId, sqlStatement, params.getColumnCount()));

            DefaultPreparedOKPacket info = new DefaultPreparedOKPacket(stmtId, fields.getColumnCount(), params.getColumnCount(), session.getWarningCount());

            if (info.getPrepareOkColumnsCount() == 0 && info.getPrepareOkParametersCount() == 0) {
                session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), true);
                return Future.succeededFuture();
            }
            session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), false);
            if (info.getPrepareOkParametersCount() > 0 && info.getPrepareOkColumnsCount() == 0) {
                for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
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
                return Future.succeededFuture();
            } else if (info.getPrepareOkParametersCount() == 0 && info.getPrepareOkColumnsCount() > 0) {
                for (int i = 0; i < info.getPrepareOkColumnsCount() ; i++) {
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
                return Future.succeededFuture();
            } else {
                for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
                    session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
                }
                session.writeColumnEndPacket(false);
                for (int i = 0; i < info.getPrepareOkColumnsCount(); i++) {
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
                return Future.succeededFuture();
            }
        } catch (Throwable throwable) {
            return Future.failedFuture(throwable);
        }
    }

    @Override
    public Future<Void> handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MySQLServerSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.appendLongData(paramId, data);
        }
        return Future.succeededFuture();
    }

    @Override
    @SneakyThrows
    public Future<Void> handlePrepareStatementExecute(long statementId, byte flags, int[] params, BindValue[] values, MySQLServerSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        SQLStatement statement = preparedStatement.getSQLStatementByBindValue(values);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("=> {}", statement);
        }
        ReceiverImpl receiver = new ReceiverImpl(session, 1, true);
        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
        return vertx.executeBlocking(promise -> MycatdbCommand.execute(dataContext, receiver, statement).onComplete(promise));
    }

    @Override
    public Future<Void> handlePrepareStatementClose(long statementId, MySQLServerSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        longPreparedStatementMap.remove(statementId);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> handlePrepareStatementFetch(long statementId, long row, MySQLServerSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public Future<Void> handlePrepareStatementReset(long statementId, MySQLServerSession session) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.resetLongData();
        }
        return session.writeOkEndPacket();
    }

    @Override
    public int getNumParamsByStatementId(long statementId, MySQLServerSession session) {
        Map<Long, PreparedStatement> prepareInfo = session.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        Objects.requireNonNull(preparedStatement);
        return preparedStatement.getParametersNumber();
    }

    @Override
    public byte[] getLongData(long statementId, int paramId, MySQLServerSession mycat) {
        PreparedStatement preparedStatement = mycat.getDataContext().getPrepareInfo().get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        ByteArrayOutputStream byteArrayOutputStream = preparedStatement.getLongData(paramId);
        if (byteArrayOutputStream == null) {
            return null;
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public BindValue[] getLastBindValue(long statementId, MySQLServerSession mycat) {
        Map<Long, PreparedStatement> prepareInfo = mycat.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        return preparedStatement.getBindValues();
    }

    @Override
    public void saveBindValue(long statementId, BindValue[] values, MySQLServerSession mycat) {
        Map<Long, PreparedStatement> prepareInfo = mycat.getDataContext().getPrepareInfo();
        PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return;
        }
        preparedStatement.setBindValues(values);
    }
}
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
import com.mysql.cj.MysqlType;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.client.InterceptorRuntime;
import io.mycat.client.UserSpace;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.metadata.MetadataManager;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.session.MycatSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chen junwen
 */
public class DefaultCommandHandler extends AbstractCommandHandler {

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
        String sql = new String(sqlBytes);
        /////////////////////////////////////////////////////
        long id = 0;
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        ResultSetBuilder fieldsBuilder = ResultSetBuilder.create();
        if (sqlStatement instanceof SQLSelectStatement) {
            List<SQLSelectItem> selectList = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock().getSelectList();
            for (SQLSelectItem sqlSelectItem : selectList) {
                SQLDataType sqlDataType = sqlSelectItem.computeDataType();
                fieldsBuilder.addColumnInfo(sqlSelectItem.toString(), sqlDataType.jdbcType());
            }
        }
        MycatRowMetaData fields = fieldsBuilder.build().getMetaData();
        ResultSetBuilder paramsBuilder = ResultSetBuilder.create();
        List<byte[]> payloads = new ArrayList<>();
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
        payloads.add(MySQLPacketUtil.generatePrepareOk(
                new DefaultPreparedOKPacket(id, fields.getColumnCount(), params.getColumnCount(), session.getWarningCount())
        ));
        if (params.getColumnCount() > 0) {
            for (int i = 1; i <= params.getColumnCount(); i++) {
                payloads.add(MySQLPacketUtil.generateColumnDefPayload(params, i));
            }
        }
        if (fields.getColumnCount() > 0) {
            for (int i = 1; i <= fields.getColumnCount(); i++) {
                payloads.add(MySQLPacketUtil.generateColumnDefPayload(fields, i));
            }
        }
        int size = payloads.size();
        for (int i = 0; i < size; i++) {
            boolean end = (i == size - 1);
            session.writeBytes(payloads.get(i), end);
        }
        return;
    }

    @Override
    public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int numParams, byte[] rest, MycatSession session) {
        session.writeColumnCount(1);
        session.writeColumnDef("1", MySQLFieldsType.FIELD_TYPE_INT24);
        session.writeRowEndPacket(false,false);
//        session.writeBinaryRowPacket(new byte[][]{"1".getBytes()});
//        session.writeOkEndPacket();
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
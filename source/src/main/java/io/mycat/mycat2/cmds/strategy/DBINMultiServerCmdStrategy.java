//package io.mycat.mycat2.cmds.strategy;
//
//import io.mycat.mycat2.MycatSession;
//import io.mycat.mycat2.cmds.*;
//import io.mycat.mycat2.cmds.multinode.DbInMultiServerCmd;
//import io.mycat.mycat2.cmds.sqlCmds.*;
//import io.mycat.mycat2.route.RouteResultset;
//import io.mycat.mycat2.route.RouteStrategy;
//import io.mycat.mycat2.route.impl.DBInMultiServerRouteStrategy;
//import io.mycat.mycat2.sqlparser.BufferSQLContext;
//import io.mycat.mysql.packet.MySQLPacket;
//import io.mycat.util.ErrorCode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
//public class DBINMultiServerCmdStrategy extends AbstractCmdStrategy {
//
//    private static final Logger logger = LoggerFactory.getLogger(DBINMultiServerCmdStrategy.class);
//
//    public static final DBINMultiServerCmdStrategy INSTANCE = new DBINMultiServerCmdStrategy();
//
//    private RouteStrategy dbInMultiServerRouteStrategy = new DBInMultiServerRouteStrategy();
//
//
//    @Override
//    protected void initMyCmdHandler() {
//        MYCOMMANDMAP.put(MySQLPacket.COM_QUIT, ComQuitCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_INIT_DB, ComInitDB.INSTANCE);
////      MYCOMMANDMAP.put(MySQLPacket.COM_QUERY,                    DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_FIELD_LIST, ComFieldListCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_CREATE_DB, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_DROP_DB, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_REFRESH, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_SHUTDOWN, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STATISTICS, ComStatisticsCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_PROCESS_INFO, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_CONNECT, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_PROCESS_KILL, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_DEBUG, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_PING, ComPingCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_TIME, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_DELAYED_INSERT, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_CHANGE_USER, ComChangeUserCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_BINLOG_DUMP, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_TABLE_DUMP, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_CONNECT_OUT, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_REGISTER_SLAVE, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_PREPARE, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_EXECUTE, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_SEND_LONG_DATA, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_CLOSE, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_RESET, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_SET_OPTION, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_STMT_FETCH, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_DAEMON, NotSupportCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_BINLOG_DUMP_GTID, DirectPassthrouhCmd.INSTANCE);
//        MYCOMMANDMAP.put(MySQLPacket.COM_RESET_CONNECTION, DirectPassthrouhCmd.INSTANCE);
//    }
//
//    @Override
//    protected void initMySqlCmdHandler() {
//        MYSQLCOMMANDMAP.put(BufferSQLContext.INSERT_SQL, DbInMultiServerCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.UPDATE_SQL, DbInMultiServerCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.DELETE_SQL, DbInMultiServerCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.DROP_SQL, DbInMultiServerCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.COMMIT_SQL, SqlComCommitCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.ROLLBACK_SQL, SqlComRollBackCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.SELECT_SQL, DbInMultiServerCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.BEGIN_SQL, SqlComBeginCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.START_SQL, SqlComStartCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.USE_SQL, SqlComStartCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.SHUTDOWN_SQL, SqlComShutdownCmd.INSTANCE);
//
//        MYSQLCOMMANDMAP.put(BufferSQLContext.SHOW_DB_SQL, ShowDbCmd.INSTANCE);
//        MYSQLCOMMANDMAP.put(BufferSQLContext.SHOW_TB_SQL, ShowTbCmd.INSTANCE);
//    }
//
//
//
//    @Override
//    protected boolean delegateRoute(MycatSession session) {
//
//        byte sqltype = session.sqlContext.getSQLType() != 0 ? session.sqlContext.getSQLType()
//                : session.sqlContext.getCurSQLType();
//
//        RouteResultset routeResultset = dbInMultiServerRouteStrategy.route(session.mycatSchema, sqltype,
//                session.sqlContext.getRealSQL(0), null, session);
//
//
//        if (routeResultset.getNodes() != null && routeResultset.getNodes().length > 1
//                && (!routeResultset.isGlobalTable() || session.sqlContext.isSelect())) {
//
//            session.setCurRouteResultset(null);
//            try {
//                logger.error(
//                        "Multi node error! Not allowed to execute SQL statement across data nodes in DB_IN_MULTI_SERVER schemaType.\n"
//                                + "Original SQL:[{}]",
//                        session.sqlContext.getRealSQL(0));
//                session.sendErrorMsg(ErrorCode.ERR_MULTI_NODE_FAILED,
//                        "Not allowed to execute SQL statement across data nodes in DB_IN_MULTI_SERVER schemaType.");
//            } catch (IOException e) {
//                session.close(false, e.getMessage());
//            }
//            return false;
//        } else {
//            session.setCurRouteResultset(routeResultset);
//        }
//        return true;
//    }
//}

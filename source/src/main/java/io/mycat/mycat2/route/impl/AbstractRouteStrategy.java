package io.mycat.mycat2.route.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.conf.SchemaBean;
import io.mycat.mycat2.beans.conf.SchemaConfig;
import io.mycat.mycat2.route.RouteResultset;
import io.mycat.mycat2.route.RouteStrategy;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

public abstract class AbstractRouteStrategy implements RouteStrategy {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouteStrategy.class);

	@Override
    public RouteResultset route(SchemaBean schema, int sqlType, String origSQL, String charset,
            MycatSession mycatSession) {

        // TODO 待处理checkSQLSchema by zhangsiwei
		//对应schema标签checkSQLschema属性，把表示schema的字符去掉
        /*
         * if (schema.isCheckSQLSchema()) { origSQL = RouterUtil.removeSchema(origSQL,
         * schema.getName()); }
         */

		/**
     * 处理一些路由之前的逻辑
     * 全局序列号，父子表插入
     */
        /*
         * if (beforeRouteProcess(schema, sqlType, origSQL, mycatSession)) { return null; }
         */

        // TODO 待处理全局表DML by zhangsiwei
		/**
		 * SQL 语句拦截
		 */
        /*
         * String stmt = MycatServer.getInstance().getSqlInterceptor().interceptSQL(origSQL,
         * sqlType); if (!origSQL.equals(stmt) && LOGGER.isDebugEnabled()) {
         * LOGGER.debug("sql intercepted to " + stmt + " from " + origSQL); }
         */

        String stmt = origSQL;
        RouteResultset rrs = new RouteResultset(stmt, sqlType);

		/**
		 * 优化debug loaddata输出cache的日志会极大降低性能
		 */
        /*
         * if (LOGGER.isDebugEnabled() && origSQL.startsWith(LoadData.loadDataHint)) {
         * rrs.setCacheAble(false); }
         */

        /**
         * rrs携带ServerConnection的autocommit状态用于在sql解析的时候遇到
         * select ... for update的时候动态设定RouteResultsetNode的canRunInReadDB属性
         */
        /*
         * if (sc != null ) { rrs.setAutocommit(sc.isAutocommit()); }
         */

		/**
		 * DDL 语句的路由
		 */
        if (BufferSQLContext.ALTER_SQL == sqlType) {
            // return RouterUtil.routeToDDLNode(rrs, sqlType, stmt, schema);
            return null;
		}

		/**
		 * 检查是否有分片
		 */
        if ((schema.getTables() == null || schema.getTables().isEmpty())
                && BufferSQLContext.SHOW_SQL != sqlType) {
            // rrs = RouterUtil.routeToSingleNode(rrs, schema.getDataNode(), stmt);
            rrs = null;
		} else {
            // RouteResultset returnedSet = routeSystemInfo(schema, sqlType, stmt, rrs);
            // if (returnedSet == null) {
            // rrs = routeNormalSqlWithAST(schema, stmt, rrs, charset, sqlType, mycatSession);
            // }
		}

		return rrs;
	}

    // TODO by zhangsiwei
	/**
	 * 路由之前必要的处理
	 * 主要是全局序列号插入，还有子表插入
	 */
    /*
     * private boolean beforeRouteProcess(SchemaConfig schema, int sqlType, String origSQL,
     * ServerConnection sc) throws SQLNonTransientException {
     * 
     * return RouterUtil.processWithMycatSeq(schema, sqlType, origSQL, sc) || (sqlType ==
     * ServerParse.INSERT && RouterUtil.processERChildTable(schema, origSQL, sc)) || (sqlType ==
     * ServerParse.INSERT && RouterUtil.processInsert(schema, sqlType, origSQL, sc)); }
     */

	/**
	 * 通过解析AST语法树类来寻找路由
	 */
    public abstract RouteResultset routeNormalSqlWithAST(SchemaBean schema, String stmt,
            RouteResultset rrs, String charset, int sqlType, MycatSession mycatSession)
            throws SQLNonTransientException;

	/**
	 * 路由信息指令, 如 SHOW、SELECT@@、DESCRIBE
	 */
    public abstract RouteResultset routeSystemInfo(SchemaBean schema, int sqlType, String stmt,
            RouteResultset rrs) throws SQLSyntaxErrorException;

	/**
	 * 解析 Show 之类的语句
	 */
	public abstract RouteResultset analyseShowSQL(SchemaConfig schema, RouteResultset rrs, String stmt)
			throws SQLNonTransientException;

}

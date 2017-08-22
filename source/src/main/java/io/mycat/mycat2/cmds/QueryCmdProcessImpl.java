package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.query.DefaultQuerySqlProcessImpl;
import io.mycat.mycat2.cmds.query.QuerySQLProcessInf;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mycat2.sqlparser.NewSQLParser;
import io.mycat.mysql.packet.MySQLPacket;

/**
 * 进行查询命令3处理
 * 
 * @since 2017年8月15日 下午6:12:18
 * @version 0.0.1
 * @author liujun
 */
public class QueryCmdProcessImpl implements SQLComandProcessInf {

	private static Logger logger = LoggerFactory.getLogger(QueryCmdProcessImpl.class);

	private NewSQLContext sqlContext = new NewSQLContext();

	private NewSQLParser sqlParser = new NewSQLParser();

	/**
	 * 查询命令工厂方法实例对象
	 */
	public static final QueryCmdProcessImpl INSTANCE = new QueryCmdProcessImpl();

	/**
	 * 分SQL进行实现
	 */
	private static final Map<Byte, QuerySQLProcessInf> QUERY_MAP = new HashMap<>();

	static {
		QUERY_MAP.put(NewSQLContext.SHOW_SQL, DefaultQuerySqlProcessImpl.INSTANCE);
		QUERY_MAP.put(NewSQLContext.SET_SQL, DefaultQuerySqlProcessImpl.INSTANCE);
		QUERY_MAP.put(NewSQLContext.SELECT_SQL, null);
		QUERY_MAP.put(NewSQLContext.INSERT_SQL, null);
		QUERY_MAP.put(NewSQLContext.UPDATE_SQL, null);
		QUERY_MAP.put(NewSQLContext.DELETE_SQL, null);
	}

	@Override
	public void commandProc(MySQLSession session) throws IOException {

		byte[] sql = session.frontBuffer.getBytes(
				session.curFrontMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize + 1,
				session.curFrontMSQLPackgInf.pkgLength - MySQLPacket.packetHeaderSize - 1);
		sqlParser.parse(sql, sqlContext);
		if (sqlContext.hasAnnotation()) {
			// 此处添加注解处理
		}

		QuerySQLProcessInf querySqlProc = null;

		for (int i = 0; i < sqlContext.getSQLCount(); i++) {
			querySqlProc = QUERY_MAP.get(sqlContext.getSQLType(i));

			if (null != querySqlProc) {
				querySqlProc.querySqlProc(session, sqlContext);
			} else {
				DefaultQuerySqlProcessImpl.INSTANCE.querySqlProc(session, sqlContext);
			}
		}
	}

}

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
	}

	@Override
	public void commandProc(MySQLSession session) throws IOException {

		byte[] sql = session.frontBuffer.getBytes(
				session.curFrontMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize + 1,
				session.curFrontMSQLPackgInf.endPos - MySQLPacket.packetHeaderSize - 1);
		sqlParser.parse(sql, sqlContext);
		if (sqlContext.hasAnnotation()) {
			// 此处添加注解处理
		}
		for (int i = 0; i < sqlContext.getSQLCount(); i++) {
			switch (sqlContext.getSQLType(i)) {
			case NewSQLContext.SHOW_SQL:
				logger.info("SHOW_SQL : " + (new String(sql, StandardCharsets.UTF_8)));
				sendSqlCommand(session);
				break;
			case NewSQLContext.SET_SQL:
				logger.info("SET_SQL : " + (new String(sql, StandardCharsets.UTF_8)));
				sendSqlCommand(session);
				break;
			case NewSQLContext.SELECT_SQL:
			case NewSQLContext.INSERT_SQL:
			case NewSQLContext.UPDATE_SQL:
			case NewSQLContext.DELETE_SQL:
				logger.info("Parse SQL : " + (new String(sql, StandardCharsets.UTF_8)));
				String tbls = "";
				for (int j = 0; j < sqlContext.getTableCount(); j++) {
					tbls += sqlContext.getTableName(j) + ", ";
				}
				logger.info("GET Tbls : " + tbls);
				// 需要单独处理的sql类型都放这里
			default:
				sendSqlCommand(session);// 线直接透传
			}
		}
	}

	private void sendSqlCommand(MySQLSession session) throws IOException {
		if (session.curSQLCommand.procssSQL(session, false)) {
			session.curSQLCommand.clearResouces(false);
		}
	}

}

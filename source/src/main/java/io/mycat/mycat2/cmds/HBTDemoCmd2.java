package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.HBT.HBTEngine;
import io.mycat.mycat2.HBT.JoinMeta;
import io.mycat.mycat2.HBT.ResultSetMeta;
import io.mycat.mycat2.HBT.RowMeta;
import io.mycat.mycat2.HBT.SqlMeta;
import io.mycat.mycat2.sqlparser.NewSQLContext;
import io.mycat.mysql.Fields;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class HBTDemoCmd2 implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(HBTDemoCmd2.class);

	public static final HBTDemoCmd2 INSTANCE = new HBTDemoCmd2();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		logger.debug("current buffer is "+session.proxyBuffer);
		/*
		 * 获取后端连接可能涉及到异步处理,这里需要先取消前端读写事件
		 */
		if(session.sqlContext.getSQLType() == NewSQLContext.SHOW_SQL) {
			session.clearReadWriteOpts();
			//session.curSQLCommand = this;
					String sql="select topic_id, question_type from e_topic where topic_id in "
							+ "('DFC22D8DC0A80B5B00039456439196','22663C93C0A80B2F00000016178188', '22663C96C0A80B2F00000022567312');";
					String sql2="select title,seq, topic_id, question_id from e_question ";
					//FetchIntoRowStream fetchIntoRowStream = new FetchIntoRowStream(new SqlMeta(sql,"a"),new RowMeta("tableA"));
					HBTEngine engine = new HBTEngine();
					String[] fieldNameList = {"e_topic.topic_id","e_topic.question_type","e_question.seq","question_id"};
					int[] fieldTypeList = {Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,};
					ResultSetMeta resultSetMeta = new ResultSetMeta(Arrays.asList(fieldNameList),
							fieldTypeList);
					
					
					String[] fieldNameList3 = {"e_topic.topic_id","e_topic.question_type","e_question.seq","e_question.question_id","item_id"};
					int[] fieldTypeList3 = {Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING,
								Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING,};

					String sql3 = "select item_id, question_id from e_item";
					ResultSetMeta resultSetMeta2 = new ResultSetMeta(Arrays.asList(fieldNameList3),
							fieldTypeList3);
					

					engine.streamOf(session, new SqlMeta(sql,"et"), new RowMeta("e_topic", "et"))
					.join(session, new SqlMeta(sql2,"eq"), new RowMeta("e_question", "eq"), 
					        new JoinMeta("e_topic.topic_id", "e_question.topic_id",HBTEngine.MEM,1), 
					        	resultSetMeta, (aRow, bRow , result)-> {
						        	result.addAll(aRow);
						        	result.add(bRow.get(0));
						        	result.add(bRow.get(2));
					        })
					.out(session);
			return true;
		}
		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {



		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		session.proxyBuffer.flip();
		session.takeOwner(SelectionKey.OP_READ);
		return true;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		//session.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		//session.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {

		return true;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		if(sessionCLosed){
			session.bufPool.recycleBuf(session.getProxyBuffer().getBuffer());
			session.unbindAllBackend();
		}
	}

	@Override
	public void clearBackendResouces(MySQLSession mysqlSession, boolean sessionCLosed) {
		if(sessionCLosed){
			mysqlSession.bufPool.recycleBuf(mysqlSession.getProxyBuffer().getBuffer());
			mysqlSession.unbindMycatSession();
		}
	}
}

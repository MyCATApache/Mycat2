package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Arrays;

import javax.swing.SortOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.CurSQLState;
import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.hbt.CountFunction;
import io.mycat.mycat2.hbt.GroupPairKeyMeta;
import io.mycat.mycat2.hbt.JoinMeta;
import io.mycat.mycat2.hbt.OrderMeta;
import io.mycat.mycat2.hbt.OutFunction;
import io.mycat.mycat2.hbt.ResultSetMeta;
import io.mycat.mycat2.hbt.RowMeta;
import io.mycat.mycat2.hbt.SqlMeta;
import io.mycat.mycat2.hbt.TableMeta;
import io.mycat.mycat2.hbt.pipeline.HBTEngine;
import io.mycat.mysql.Fields;
import io.mycat.proxy.ProxyBuffer;

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
		//if(session.sqlContext.getSQLType() == NewSQLContext.SHOW_SQL) {
			session.clearReadWriteOpts();
			//session.curSQLCommand = this;
			String sql="select topic_id, question_type, update_time from e_topic ";
//					String sql="select topic_id, question_type, update_time from e_topic where topic_id in "
//							+ "('1DDA8F5EC0A82CB200000817177360','1DDA8E99C0A82CB200000543452550','1DDA8E9EC0A82CB200000549901449', '1DDA8EB3C0A82CB200000573846014');";
					String sql2="select title,seq, topic_id, question_id from e_question ";
					//FetchIntoRowStream fetchIntoRowStream = new FetchIntoRowStream(new SqlMeta(sql,"a"),new RowMeta("tableA"));
					HBTEngine engine = new HBTEngine();
					String[] fieldNameList = {"topic_id", "question_type", "update_time", "seq", "question_id"};
					int[] fieldTypeList = {Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING,};
					ResultSetMeta resultSetMeta = new ResultSetMeta(Arrays.asList(fieldNameList),
							fieldTypeList);
					
					
//					String[] fieldNameList3 = {"e_topic.topic_id","e_topic.question_type","e_question.seq","e_question.question_id","item_id"};
//					int[] fieldTypeList3 = {Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING,
//								Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING, Fields.FIELD_TYPE_STRING,};
//
//					String sql3 = "select item_id, question_id from e_item";
//					ResultSetMeta resultSetMeta2 = new ResultSetMeta(Arrays.asList(fieldNameList3),
//							fieldTypeList3);
					

					
					String[] groupSetMetaList = {"count", "topic_id", "question_type", "update_time", "question_id"};
					int[] groupSetMetaTypeList = {Fields.FIELD_TYPE_INT24,Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,Fields.FIELD_TYPE_STRING,};
					
			    	ResultSetMeta groupResultSetMeta = new ResultSetMeta(Arrays.asList(groupSetMetaList),
			    			groupSetMetaTypeList);
			    	GroupPairKeyMeta groupPairKeyMeta =	new GroupPairKeyMeta("topic_id", "update_time");
			    	
					engine.streamOf(session, new SqlMeta(sql,"et"), new RowMeta("e_topic", "et"))
					.join(session, new SqlMeta(sql2,"eq"), new RowMeta("e_question", "eq"), 
					        new JoinMeta("e_topic.topic_id", "e_question.topic_id", HBTEngine.MEM, 100), 
					        	resultSetMeta, (aRow, bRow, result) -> {
						        	result.addAll(aRow);
						        	result.add(bRow.get(1));
						        	result.add(bRow.get(2));
					        }).group( groupPairKeyMeta, groupResultSetMeta, Arrays.asList(new CountFunction(), new OutFunction(0,1,2,3)))
					.order(new OrderMeta(Arrays.asList( "update_time" ,"count" ,"topic_id"), Arrays.asList(SortOrder.DESCENDING, SortOrder.ASCENDING, SortOrder.ASCENDING)))
					.limit(11)
					.out(session);
			return true;
		//}
		//return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) {



		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		TableMeta tableMeta = (TableMeta) session.curSQLSate.get(CurSQLState.HBT_TABLE_META);
		
		if(null != tableMeta && !tableMeta.isWriteFinish()) {
			ProxyBuffer buffer = session.proxyBuffer;
			buffer.reset();
			tableMeta.writeRowData(buffer);
			buffer.flip();
			buffer.readIndex = buffer.writeIndex; 
			session.takeOwner(SelectionKey.OP_WRITE);
			try {
				session.writeToChannel();
			} catch (IOException e) {
				e.printStackTrace();
			} 
			return false;
		} else {
			session.curSQLSate.remove(CurSQLState.HBT_TABLE_META);
			session.proxyBuffer.flip();
			session.takeOwner(SelectionKey.OP_READ);
			return true;
		}
		
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		//session.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		//session.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) {

		return true;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		if(sessionCLosed){
			session.recycleAllocedBuffer(session.getProxyBuffer());
			session.unbindAllBackend();
		}
	}

	@Override
	public void clearBackendResouces(MySQLSession mysqlSession, boolean sessionCLosed) {
		if(sessionCLosed){
			mysqlSession.recycleAllocedBuffer(mysqlSession.getProxyBuffer());
			mysqlSession.unbindMycatSession();
		}
	}
}

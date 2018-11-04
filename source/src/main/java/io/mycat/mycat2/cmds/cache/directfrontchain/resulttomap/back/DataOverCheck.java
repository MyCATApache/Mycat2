package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;

/**
 * 数据结束检查
 * 
 * @since 2017年9月22日 下午4:48:36
 * @version 0.0.1
 * @author liujun
 */
public class DataOverCheck implements ChainExecInf {
	
	/**
	 * 实例对象
	 */
	public static final DataOverCheck INSTANCE = new DataOverCheck();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		// 首先获取mysqlsesssion对象
		MySQLSession mysqlSession = (MySQLSession) seqList.getSession();

		// 进行当前的数据检查是否结束
//		boolean nextReadFlag = false;
//		do {
//			// 进行报文的处理流程
//			nextReadFlag = mysqlSession.getMycatSession().commandHandler.procss(mysqlSession);
//		} while (nextReadFlag);

		return seqList.nextExec();
	}

}

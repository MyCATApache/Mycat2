package io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;

/**
 * 前端数据结束检查
 * 
 * @since 2017年9月22日 下午4:48:36
 * @version 0.0.1
 * @author liujun
 */
public class FrontDataOverCheck implements ChainExecInf {

	/**
	 * 前段实例对象
	 */
	public static final FrontDataOverCheck INSTANCE = new FrontDataOverCheck();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		// 首先获取mysqlsesssion对象
		MycatSession session = (MycatSession) seqList.getSession();

		// 进行当前的数据检查是否结束
		boolean nextReadFlag = false;
		do {
			// 进行报文的处理流程
			nextReadFlag = session.curBackend.commandHandler.procss(session.curBackend);
		} while (nextReadFlag);

		return seqList.nextExec();
	}

}

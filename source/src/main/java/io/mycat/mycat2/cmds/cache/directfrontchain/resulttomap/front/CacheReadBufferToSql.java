package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.front;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 读取buffer信息，将其转换为SQL，目前使用固定读取，真实将，需要由SQL解析后
 * 
 * @since 2017年9月18日 下午4:09:41
 * @version 0.0.1
 * @author liujun
 */
public class CacheReadBufferToSql implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final CacheReadBufferToSql INSTANCE = new CacheReadBufferToSql();

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mycat.mycat2.common.ChainExecInf#invoke(io.mycat.mycat2.common.
	 * SeqContextList)
	 */
	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		MycatSession mycatSession = (MycatSession) seqList.getSession();

		ProxyBuffer proxyBuf = mycatSession.proxyBuffer;

		// 将当前的SQL信息放入到session中
		String selectSql = (String) mycatSession.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CACHE_SQL_STR.getKey());

		// 清理buffer，重新组装查询的报文
		proxyBuf.reset();

		QueryPacket queryPkg = new QueryPacket();

		queryPkg.packetId = 0;
		queryPkg.packetLength = selectSql.getBytes().length;
		queryPkg.sql = selectSql;

		// 查询数据的sql
		queryPkg.write(proxyBuf);

		// 如果没有 ，则结束操作
		return seqList.nextExec();
	}

}

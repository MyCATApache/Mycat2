package io.mycat.mycat2.cmds.sqlCmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front.CacheGetProcess;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front.CacheQueryResultSetProc;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front.CacheResultProc;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.front.FrontDataOverCheck;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.largeresult.DataEventProc;
import io.mycat.mycat2.cmds.cache.directfrontchain.maptoresult.largeresult.ResultOverFlag;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back.BackEventProc;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back.CacheFlowCheck;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back.DataOverCheck;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back.FrontEventProc;
import io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back.ResultSetDatatoMapFile;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKeyEnum;

/**
 * 进行缓存的文件处理
 * 
 * @since 2017年9月4日 下午6:53:05
 * @version 0.0.1
 * @author liujun
 */
public class CacheMapFileCommand implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(CacheMapFileCommand.class);

	/**
	 * 透传的实例对象
	 */
	public static final CacheMapFileCommand INSTANCE = new CacheMapFileCommand();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {

		SeqContextList seqcontext = (SeqContextList) session.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey());

		if (null == seqcontext) {
			seqcontext = new SeqContextList();
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey(), seqcontext);
		}

		seqcontext.clear();

		seqcontext.setSession(session);
		// 检查当前的缓存标识
		seqcontext.addExec(CacheGetProcess.INSTANCE);
		// 如果需要走缓存，则先进行当前数据集是否结束的检查
		seqcontext.addExec(FrontDataOverCheck.INSTANCE);
		// 进发当前buffer读取完成后的事件处理
		seqcontext.addExec(CacheResultProc.INSTANCE);

		try {
			seqcontext.nextExec();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {

		// 首先进行一次报文的读取操作
		if (!session.readFromChannel()) {
			return false;
		}

		SeqContextList seqcontext = (SeqContextList) session.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey());

		if (null == seqcontext) {
			seqcontext = new SeqContextList();
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey(), seqcontext);
		}

		seqcontext.clear();

		seqcontext.setSession(session);
		// 首先检查是否需要缓存
		seqcontext.addExec(CacheFlowCheck.INSTANCE);
		// 如果需要走缓存，则先进行当前数据集是否结束的检查
		seqcontext.addExec(DataOverCheck.INSTANCE);
		// 检查检查文件是否为结果集，如果为结果集，则将数据写入缓存
		seqcontext.addExec(ResultSetDatatoMapFile.INSTANCE);
		// 后端事件处理
		seqcontext.addExec(BackEventProc.INSTANCE);
		// 前段事件处理
		seqcontext.addExec(FrontEventProc.INSTANCE);

		try {
			seqcontext.nextExec();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			seqcontext.clear();
		}
		return false;
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {

		SeqContextList seqcontext = (SeqContextList) session.getSessionAttrMap()
				.get(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey());

		if (null == seqcontext) {
			seqcontext = new SeqContextList();
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CACHE_MYCAT_CHAIN_SEQ.getKey(), seqcontext);
		}

		seqcontext.clear();

		seqcontext.setSession(session);
		// 检查当前是否结束
		seqcontext.addExec(ResultOverFlag.INSTANCE);
		// 检查是否需要更新缓存 的结果集操作
		seqcontext.addExec(CacheQueryResultSetProc.INSTANCE);
		// 读取缓存中的数据
		seqcontext.addExec(CacheGetProcess.INSTANCE);
		// 检查当前数据是否结束
		seqcontext.addExec(FrontDataOverCheck.INSTANCE);
		// 进行数据的结束 处理
		seqcontext.addExec(DataEventProc.INSTANCE);

		try {
			seqcontext.nextExec();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			seqcontext.clear();
		}

		return false;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 向后端写入完成数据后，则从后端读取数据
		session.proxyBuffer.flip();
		// 由于单工模式，在向后端写入完成后，需要从后端进行数据读取
		session.change2ReadOpts();
		return false;
	}

	@Override
	public void clearFrontResouces(MycatSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearBackendResouces(MySQLSession session, boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

}

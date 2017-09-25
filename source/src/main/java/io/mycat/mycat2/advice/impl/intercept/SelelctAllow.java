package io.mycat.mycat2.advice.impl.intercept;

import java.io.IOException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.DefaultInvocation;

public class SelelctAllow extends DefaultInvocation implements Function<MycatSession,Boolean>{
	
	private boolean isallow = true;
	
	public static final SelelctAllow INSTANCE = new SelelctAllow();
	
	private static final Logger logger = LoggerFactory.getLogger(SelelctAllow.class);
	
	/**
	 * 组装 mysqlCommand
	 */
	@Override
	public Boolean apply(MycatSession session) {
		setCommand(session.curSQLCommand);
		session.curSQLCommand = this;
		return Boolean.TRUE;
	}
	
	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		logger.debug("========================> SelelctAllow {}",session.sqlContext.getRealSQL(0));
		if(isallow){  // 允许 执行 继续调用责任链
			return super.procssSQL(session);
		}else{        // 不允许执行  调用自身的command handler
			session.curSQLCommand = this;
			return super.procssSQL(session);
		}
	}
}

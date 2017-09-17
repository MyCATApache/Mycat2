package io.mycat.mycat2.Interceptor.impl;

import io.mycat.mycat2.MyCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.Interceptor.Interceptor;
import io.mycat.mycat2.cmds.BlockSqlCmd;

/**
 *  对sql进行拦截
 * @author zwy
 */
public class BlockSQLIntercepor   implements Interceptor  {
	public static final Interceptor INSTANCE = new BlockSQLIntercepor();
	@Override
	public boolean intercept(MycatSession mycatSession) {
		mycatSession.putSQLCmd(this, BlockSqlCmd.INSTANCE);
		return true;
	}

}

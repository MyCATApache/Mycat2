package io.mycat.mycat2.Interceptor;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.proxy.AbstractSession;

/**
 * 
 * @author zwy
 *
 */
public interface Interceptor {

	/**
	 * 
	 * @param mycatSession
	 * @return boolean
	 * next interceptor是否可以执行或者改变sqlCmd
	 * @throws IOException 
	 */
	boolean intercept(MycatSession mycatSession ) throws IOException;
	
}

package io.mycat.mycat2;

import java.io.IOException;

/**
 * 负责处理SQL命令
 * @author wuzhihui
 *
 * @param <T>
 */
public interface SQLCommand {

	
	/**
	 * 直接应答请求报文，如果是直接应答的，则此方法调用一次就完成了，如果是靠后端响应后才应答，则至少会调用两次，
	 * 第二次的参数backresReceived为true，表示收到后端响应
	 * @param session
	 * @return 是否完成了应答
	 */
	public boolean procssSQL(MySQLSession session,boolean backresReceived) throws IOException;
	
	/**
	 * 清理资源，只清理自己产生的资源（如创建了Buffer，以及Session中放入了某些对象）
	 * @param socketClosed 是否因为Session关闭而清理资源，此时应该彻底清理
	 */
    public void clearResouces(boolean sessionCLosed);	
	
	
}

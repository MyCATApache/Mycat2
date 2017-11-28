package io.mycat.mycat2.cmds.pkgread;

/**
 * 用来进行将前端的命令与后端的结束处理类相绑定
 * 
 * @since 2017年11月20日 下午10:53:56
 * @version 0.0.1
 * @author liujun
 */
public class HandlerAdapter {

	/**
	 * 实例对象
	 */
	public static final HandlerAdapter INSTANCE = new HandlerAdapter();

	/**
	 * 进行前段与后端结束的相验证的关系，索引为请求包的类型
	 */
	private static final CommandHandler[] HANDLERS = new CommandHandler[33];

	// 指定关系
	static {
		// 0x01 COM_QUIT 关闭连接
		HANDLERS[0x01] = CommQueryHandlerAdapter.INSTANCE;
		// 0x02 COM_INIT_DB 切换数据库
		HANDLERS[0x02] = CommQueryHandlerAdapter.INSTANCE;
		// 0x03 COM_QUERY SQL查询请求
		HANDLERS[0x03] = CommQueryHandlerAdapter.INSTANCE;
		// 0x16 COM_STMT_PREPARE 预处理SQL语句
		HANDLERS[0x16] = ComStmtPrepareHeaderHandlerAdapter.INSTANCE;
		// 0x17 COM_STMT_EXECUTE 执行预处理语句
		HANDLERS[0x17] = ComStmtExecuteHeaderHandlerAdapter.INSTANCE;
		// 0x18 COM_STMT_SEND_LONG_DATA 发送BLOB类型的数据
		// 0x19 COM_STMT_CLOSE 销毁预处理语句
		HANDLERS[0x19] = CommQueryHandlerAdapter.INSTANCE;
		// 0x1A COM_STMT_RESET 清除预处理语句参数缓存
		HANDLERS[0x1A] = ComStmtResetHandlerAdapter.INSTANCE;
		// 0x1C COM_STMT_FETCH 获取预处理语句的执行结果
		HANDLERS[0x1C] = ComStmtPrepareHandlerAdapter.INSTANCE;
	}

	/**
	 * 根据前段请求包的类型，获取后端报文结束处理类
	 * 
	 * @param type
	 * @return
	 */
	public CommandHandler getHandlerByType(int type) {
		if (type <= HANDLERS.length) {
			return HANDLERS[type];
		}

		return null;
	}
}

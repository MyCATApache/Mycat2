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
	private static final CommandHandlerAdapter[] HANDLERS = new CommandHandlerAdapter[33];

	// 指定关系
	static {
		// 0x01 COM_QUIT 关闭连接
		HANDLERS[1] = CommQueryHandlerAdapter.INSTANCE;
		// 0x02 COM_INIT_DB 切换数据库
		HANDLERS[2] = CommQueryHandlerAdapter.INSTANCE;
		// 0x03 COM_QUERY SQL查询请求
		HANDLERS[3] = CommQueryHandlerAdapter.INSTANCE;

	}

	/**
	 * 根据前段请求包的类型，获取后端报文结束处理类
	 * 
	 * @param type
	 * @return
	 */
	public CommandHandlerAdapter getHandlerByType(int type) {
		if (type <= HANDLERS.length) {
			return HANDLERS[type];
		}

		return null;
	}
}

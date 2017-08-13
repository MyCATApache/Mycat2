package io.mycat.proxy.man;

/**
 * 管理命令的相关错误代码常量
 * 
 * @author wuzhihui
 *
 */
public class ManErrorCode {
	// 客户端错误代码,客户端的错误从400-499，
	public static final int CLIENT_NORMAL_ERROR = 400;

	// 服务端错误代码，服务端的错误从500开始
	public static final int SERVER_NORMAL_ERROR = 500;

	// 希望有人实现这个特性，提示信息建议为：Need implemented ,To by a Hero or Bear, It's your
	// choice!
	public static final int NEED_IMPLEMENTED_ERROR = 666;

	// 可能是Bug，需要修复，提示信息建议为：Maybe a bug ,Leader us want you fix it
	public static final int MAYBE_BUG_ERROR = 800;
}

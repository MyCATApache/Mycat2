package io.mycat.mycat2.cmds.judge;

/**
 * 服务器状态的枚举信息
 * 
 * @since 2017年5月14日 下午3:25:00
 * @version 0.0.1
 * @author liujun
 */
public enum ServerStatusEnum {

	/**
	 * 检查in transation是否设置
	 * 
	 * 用于标识事务当前是否活动的
	 */
	IN_TRANSACTION(0),

	/**
	 * 是否设置了自动提交
	 * 
	 * Autocommit mode is set
	 * 
	 * 
	 * 自动提交模式设置
	 */
	AUTO_COMMIT(1),

	/**
	 * 是否返回更多的结果
	 * 
	 * more results exists (more packet follow)
	 * 
	 * 更多的结果存在(更多的包跟随)
	 */
	MORE_RESULTS(2),

	/**
	 * 多个结果集
	 * 
	 * 一个SQL发送多条SQL语句，以此为标识是多个查询
	 */
	MULIT_QUERY(3),

	/**
	 * 设置bad index used
	 * 
	 */
	BAD_INDEX_USED(4),

	/**
	 * 索引
	 */
	NO_INDEX_Used(5),

	/**
	 * 参数
	 * 
	 * when using COM_STMT_FETCH, indicate that current cursor still has result
	 * 
	 * 在发送COM_STMT_FETCH命令时，指出当前游标仍然有结果
	 */
	CURSOR_EXISTS(6),

	/**
	 * 进行检查
	 * 
	 * when using COM_STMT_FETCH, indicate that current cursor has finished to
	 * send results 
	 * 
	 * 在使用COM_STMT_FETCH发送命令时，指示当前游标已完成发送结果
	 * 
	 */
	LAST_ROW_SENT(7),

	/**
	 * 数据库删除检查
	 * 
	 * database has been dropped
	 * 
	 * 数据库已经被删除
	 */
	DATABASE_DROPPED(8),

	/**
	 * 
	 * 
	 * current escape mode is "no backslash escape"
	 * 
	 * 当前的转义模式是“无反斜杠转义”
	 * 
	 */
	NO_BACKSLASH_ESCAPES(9),

	/**
	 * 会话检查
	 * 
	 * Session change type
	 * 
	 * 0 SESSION_TRACK_SYSTEM_VARIABLES
	 * 
	 * 1 SESSION_TRACK_SCHEMA
	 * 
	 * 2 SESSION_TRACK_STATE_CHANGE
	 * 
	 * 3 SESSION_TRACK_GTIDS
	 * 
	 * 4 SESSION_TRACK_TRANSACTION_CHARACTERISTICS
	 * 
	 * 5 SESSION_TRACK_TRANSACTION_STATE
	 * 
	 */
	SESSION_STATE_CHECK(10),

	/**
	 * 检查
	 */
	QUERY_WAS_SLOW(11),

	/**
	 * 参数
	 */
	PS_OUT_PARAMS(12),;

	/**
	 * 状态位信息
	 */
	private int statusBit;

	private ServerStatusEnum(int statusBit) {
		this.statusBit = statusBit;
	}

	public int getStatusBit() {
		return statusBit;
	}

	public void setStatusBit(int statusBit) {
		this.statusBit = statusBit;
	}

	/**
	 * 进行状态的检查
	 * 
	 * @param value
	 *            状态值
	 * @param status
	 *            比较的状态枚举
	 * @return true 状态中有设置，否则为未设置
	 */
	public static boolean StatusCheck(int value, ServerStatusEnum status) {
		int tempVal = 1 << status.getStatusBit();
		if ((value & tempVal) == tempVal) {
			return true;
		}
		return false;
	}

	public static void main(String[] args) {
		boolean result = ServerStatusEnum.StatusCheck(0x0003, ServerStatusEnum.IN_TRANSACTION);
		System.out.println(result);
		boolean resultauto = ServerStatusEnum.StatusCheck(0x0003, ServerStatusEnum.AUTO_COMMIT);
		System.out.println(resultauto);
		boolean resulresult = ServerStatusEnum.StatusCheck(0x0003, ServerStatusEnum.MORE_RESULTS);
		System.out.println(resulresult);
	}

}

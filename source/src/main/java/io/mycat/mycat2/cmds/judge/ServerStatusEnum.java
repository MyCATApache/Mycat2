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
	 */
	IN_TRANSACTION(0),

	/**
	 * 是否设置了自动提交
	 */
	AUTO_COMMIT(1),

	/**
	 * 是否返回更多的结果
	 */
	MORE_RESULTS(2),

	/**
	 * 多个结果集
	 */
	MULT_QUERY(3),

	/**
	 * 设置bad index used
	 */
	BAD_INDEX_USED(4),

	/**
	 * 索引
	 */
	NO_INDEX_Used(5),

	/**
	 * 参数
	 */
	CURSOR_EXISTS(6),

	/**
	 * 进行检查
	 */
	LAST_ROW_SENT(7),

	/**
	 * 数据库删除检查
	 */
	DATABASE_DROPPED(8),

	/**
	 * 空间检查
	 */
	NO_BACKSLASH_ESCAPES(9),

	/**
	 * 会话检查
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

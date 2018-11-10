package io.mycat.mycat2.cmds.judge;

/**
 * 服务器状态的枚举信息
 *
 * @author liujun cjw
 * @version 0.0.1
 * @since 2017年5月14日 下午3:25:00
 */
public final class ServerStatus {

    /**
     * 检查in transation是否设置
     * <p>
     * 用于标识事务当前是否活动的
     */
    public static final int IN_TRANSACTION = 1;

    /**
     * 是否设置了自动提交
     * <p>
     * Autocommit mode is set
     * <p>
     * <p>
     * 自动提交模式设置
     */
    public static final int AUTO_COMMIT = 1 << 1;

    /**
     * 是否返回更多的结果
     * <p>
     * more results exists (more packet follow)
     * <p>
     * 更多的结果存在(更多的包跟随)
     */
    public static final int MORE_RESULTS = 1 << 2;

    /**
     * 多个结果集
     * <p>
     * 一个SQL发送多条SQL语句，以此为标识是多个查询
     */
    public static final int MULIT_QUERY = 1 << 3;

    /**
     * 设置bad index used
     */
    public static final int BAD_INDEX_USED = 1 << 4;

    /**
     * 索引
     */
    public static final int NO_INDEX_Used = 1 << 5;

    /**
     * 参数
     * <p>
     * when using COM_STMT_FETCH, indicate that current cursor still has result
     * <p>
     * 在发送COM_STMT_FETCH命令时，指出当前游标仍然有结果
     */
    public static final int CURSOR_EXISTS = 1 << 6;

    /**
     * 进行检查
     * <p>
     * when using COM_STMT_FETCH, indicate that current cursor has finished to
     * send results
     * <p>
     * 在使用COM_STMT_FETCH发送命令时，指示当前游标已完成发送结果
     */
    public static final int LAST_ROW_SENT =1<<7;

    /**
     * 数据库删除检查
     * <p>
     * database has been dropped
     * <p>
     * 数据库已经被删除
     */
    public static final int  DATABASE_DROPPED  = 1<<8;

    /**
     * current escape mode is "no backslash escape"
     * <p>
     * 当前的转义模式是“无反斜杠转义”
     */
    public static final int  NO_BACKSLASH_ESCAPES = 1<<9;

    /**
     * 会话检查
     * <p>
     * Session change type
     * <p>
     * 0 SESSION_TRACK_SYSTEM_VARIABLES
     * <p>
     * 1 SESSION_TRACK_SCHEMA
     * <p>
     * 2 SESSION_TRACK_STATE_CHANGE
     * <p>
     * 3 SESSION_TRACK_GTIDS
     * <p>
     * 4 SESSION_TRACK_TRANSACTION_CHARACTERISTICS
     * <p>
     * 5 SESSION_TRACK_TRANSACTION_STATE
     */
    public static final int SESSION_STATE_CHECK = 1<<10;

    /**
     * 检查
     */
    public static final int QUERY_WAS_SLOW = 1<<11;

    /**
     * 参数
     */
    public static final int  PS_OUT_PARAMS = 1<<12;

    /**
     * 进行状态的检查
     *
     * @param value  状态值
     * @param status 比较的状态枚举
     * @return true 状态中有设置，否则为未设置
     */
    public static boolean statusCheck(int value, int tempVal) {
        return (value & tempVal) == tempVal;
    }

    public static void main(String[] args) {
        boolean result = ServerStatus.statusCheck(0x0003, ServerStatus.IN_TRANSACTION);
        System.out.println(result);
        boolean resultauto = ServerStatus.statusCheck(0x0003, ServerStatus.AUTO_COMMIT);
        System.out.println(resultauto);
        boolean resulresult = ServerStatus.statusCheck(0x0003, ServerStatus.MORE_RESULTS);
        System.out.println(resulresult);
    }

}

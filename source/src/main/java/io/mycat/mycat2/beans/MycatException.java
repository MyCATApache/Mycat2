package io.mycat.mycat2.beans;

/**
 * cjw
 * 294712221@qq.com
 * 用于mycat处理逻辑的异常,在线程级别捕获,捕获此异常后构造错误的响应包返回,之后清理session相关所有资源
 * 在开发阶段
 */
public class MycatException extends RuntimeException {
    public MycatException(String format,Object... args) {
        this(String.format(format, args));
    }

    public MycatException(String message) {
        super(message);
    }

    public MycatException(String message, Throwable cause) {
        super(message, cause);
    }

    public MycatException(Throwable cause) {
        super(cause);
    }

    public MycatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

package io.mycat.util.packet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 写任务
 * 上下文 #{@link io.mycat.MycatDataContext}
 * 日志 #{@link org.slf4j.MDC}
 * 事务 #{@link io.mycat.TransactionSession}
 * 异常埋点
 * 执行耗时
 * 监控不同数据包的发送次数
 *
 * @author wangzihaogithub 2021-01-21
 */
public abstract class AbstractSocketWritePacket implements Runnable{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public final void run() {
        writeBefore();
        writeToSocket();
        writeAfter();
    }

    public void writeBefore(){
        logger.debug("writeBefore");
    }

    public void writeAfter(){
        logger.debug("writeAfter");
    }

    public abstract void writeToSocket();

}
package io.mycat.vertxmycat;

import io.mycat.Process;
import io.mycat.ProcessImpl;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import lombok.AllArgsConstructor;

/**
 * 追踪, 转移, 记录资源信息.
 *
 * @author wangzihaogithub 2021年5月1日22:53:01
 */
public class MycatVertxMetricsFactory implements VertxMetricsFactory {
    @Override
    public VertxMetrics metrics(VertxOptions options) {
        return new MycatVertxMetrics(options);
    }

    @AllArgsConstructor
    public static class MycatVertxMetrics implements VertxMetrics {
        private final VertxOptions options;

        @Override
        public PoolMetrics<Process> createPoolMetrics(String poolType, String poolName, int maxPoolSize) {
            return new MycatPoolMetrics(options);
        }
    }

    /**
     * 切换线程前后,保存转移上下文
     * 用于实现 show processlist, 和 kill 命令
     */
    @AllArgsConstructor
    public static class MycatPoolMetrics implements PoolMetrics<Process> {
        private final VertxOptions options;

        /**
         * 在创建任务的线程里回调
         *
         * @return
         */
        @Override
        public Process submitted() {
            Process process = Process.getCurrentProcess();
            if (process == Process.EMPTY) {
                // 父子线程传递
                process = new ProcessImpl();
            }
            return process;
        }

        /**
         * 在执行任务的线程里回调
         *
         * @param process
         * @return
         */
        @Override
        public Process begin(Process process) {
            process.retain();
            Process.setCurrentProcess(process);
            return process;
        }

        /**
         * 在创建任务的线程里回调
         *
         * @param process
         */
        @Override
        public void rejected(Process process) {
            //todo 拒绝策略 2021年5月2日00:22:24
        }

        /**
         * 在执行任务的线程里回调
         *
         * @param process
         * @param succeeded {@link Future#succeeded()}
         */
        @Override
        public void end(Process process, boolean succeeded) {
            process.release();
        }

        /**
         * 线程池销毁时
         */
        @Override
        public void close() {

        }
    }
}

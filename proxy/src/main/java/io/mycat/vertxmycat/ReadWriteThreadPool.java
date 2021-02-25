package io.mycat.vertxmycat;

import io.mycat.NameableThreadFactory;
import lombok.SneakyThrows;

import java.util.concurrent.*;

/**
 * 读写线程池
 *
 * @author wangzihaogithub 2021-02-25
 */
public class ReadWriteThreadPool {
    private final ThreadPoolExecutor readThreadPool;
    private final ThreadPoolExecutor writeThreadPool;
    private int outOfThreadsBlockTimeoutMs;

    public ReadWriteThreadPool(String name, int maxReadThreads, int maxWriteThreads, int outOfThreadsBlockTimeoutMs) {
        long keepAliveTime = 60L;
        this.outOfThreadsBlockTimeoutMs = outOfThreadsBlockTimeoutMs;
        RejectedExecutionHandler handler = new BlockRejectedExecutionHandler();
        this.readThreadPool = new ThreadPoolExecutor(readCoreSize(maxReadThreads), maxReadThreads,
                keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NameableThreadFactory(name + "-read", false),
                handler);
        this.writeThreadPool = new ThreadPoolExecutor(writeCoreSize(maxWriteThreads), maxWriteThreads,
                keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NameableThreadFactory(name + "-write", false),
                handler);
    }

    public void execute(boolean read, Runnable runnable) throws RejectedExecutionException {
        if (read) {
            readThreadPool.execute(runnable);
        } else {
            writeThreadPool.execute(runnable);
        }
    }

    public void shutdown() {
        readThreadPool.shutdown();
        writeThreadPool.shutdown();
    }

    private int readCoreSize(int maxThreads) {
        int coreSize = Math.max(maxThreads / 5, 1);
        return coreSize > 100 ? 100 : coreSize;
    }

    private int writeCoreSize(int maxThreads) {
        int coreSize = Math.max(maxThreads / 5, 1);
        return coreSize > 100 ? 100 : coreSize;
    }

    public class BlockRejectedExecutionHandler implements RejectedExecutionHandler {
        @SneakyThrows
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            boolean offer = executor.getQueue().offer(r, outOfThreadsBlockTimeoutMs, TimeUnit.MILLISECONDS);
            if (!offer) {
                throw new RejectedExecutionException("Task " + r.toString() +
                        " rejected from " +
                        executor.toString());
            }
        }
    }

    public static void main(String[] args) {
        ReadWriteThreadPool threadPool = new ReadWriteThreadPool("jdbc", 1, 1,
                1000);
        threadPool.execute(true, () -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        try {
            threadPool.execute(true, () -> {
                System.out.println("threadPool = " + threadPool);
            });
        } catch (RejectedExecutionException e) {
            // 等待阻塞1秒后，抛出拒绝异常
            e.printStackTrace();
        }
        threadPool.shutdown();
    }
}

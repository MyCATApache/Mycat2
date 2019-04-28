package io.mycat.memory;


import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.memory.MemoryAllocator;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;

public abstract class MemoryManager {

    @GuardedBy("this")
    protected ExecutionMemoryPool onHeapExecutionMemoryPool =
            new ExecutionMemoryPool(this, MemoryMode.ON_HEAP);

    @GuardedBy("this")
    protected ExecutionMemoryPool offHeapExecutionMemoryPool =
            new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP);

//    public static final long bufferPageSize = 0L;
    protected final long maxOffHeapMemory ;
//    protected final long offHeapExecutionMemory = 0L;
    private int numCores = 0;



    private final Map<MemoryConf, Object> conf;

    public MemoryManager(Map<MemoryConf, Object> conf, int numCores, long onHeapExecutionMemory) {
        this.conf = conf;
        this.numCores = numCores;
        this.maxOffHeapMemory =  (Long)conf.get(MemoryConf.MEMORY_OFF_HEAP_SIZE);
        onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory);
        offHeapExecutionMemoryPool.incrementPoolSize((Long) conf.get(MemoryConf.OFF_HEAP_STORAGE_MEMORY));
    }

    protected abstract long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode);

    /**
     * Release numBytes of execution memory belonging to the given task.
     */
    public void releaseExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        synchronized (this) {
            switch (memoryMode) {
                case ON_HEAP:
                    onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
                    break;
                case OFF_HEAP:
                    offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
                    break;
            }
        }

    }


    /**
     * Execution memory currently in use, in bytes.
     */
    public final long executionMemoryUsed() {
        synchronized (this) {
            return (onHeapExecutionMemoryPool.memoryUsed() + offHeapExecutionMemoryPool.memoryUsed());
        }
    }

    public long releaseAllExecutionMemoryForTask(Long taskAttemptId) {
        synchronized (this) {
            return onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) + offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId);
        }
    }


    /**
     * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
     * sun.misc.Unsafe.
     */
    public final MemoryMode tungstenMemoryMode() {
        if (Boolean.TRUE.equals(conf.get(MemoryConf.MEMORY_OFF_HEAP_ENABLED))) {
            Long aLong = (Long) conf.get(MemoryConf.MAX_OFF_HEAP_MEMORY);
            assert (aLong > 0);
            assert (Platform.unaligned());
            return MemoryMode.OFF_HEAP;
        } else {
            return MemoryMode.ON_HEAP;
        }
    }
    /**
     *  On heap execution memory currently in use, in bytes.
     */
    public  final long onHeapExecutionMemoryUsed() {
        synchronized (this){
            return onHeapExecutionMemoryPool.memoryUsed();
        }
    }

    /**
     *  Off heap execution memory currently in use, in bytes.
     */
    public   final long offHeapExecutionMemoryUsed() {
        synchronized (this) {
           return offHeapExecutionMemoryPool.memoryUsed();
        }
    }
    /**
     * The default page size, in bytes.
     * <p>
     * If user didn't explicitly set "mycat.buffer.pageSize", we figure out the default value
     * by looking at the number of cores available to the process, and the total amount of memory,
     * and then divide it by a factor of safety.
     */
    public long pageSizeBytes() {

        long minPageSize = 1L * 1024 * 1024;  // 1MB
        long maxPageSize = 64L * minPageSize; // 64MB
        int cores = (numCores > 0) ? numCores : Runtime.getRuntime().availableProcessors();

        // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
        int safetyFactor = 16;
        long maxTungstenMemory = 0L;
        switch (tungstenMemoryMode()) {
            case ON_HEAP:
                maxTungstenMemory = onHeapExecutionMemoryPool.poolSize();
                break;
            case OFF_HEAP:
                maxTungstenMemory = offHeapExecutionMemoryPool.poolSize();
                break;
        }

        long size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor);
        Long defaultValue = Math.min(maxPageSize, Math.max(minPageSize, size));

        return (Long) conf.getOrDefault(MemoryConf.BUFFER_PAGE_SIZE, defaultValue);
    }

    /**
     * Allocates memory for use by Unsafe/Tungsten code.
     */
    public final MemoryAllocator tungstenMemoryAllocator() {
        switch (tungstenMemoryMode()) {
            case ON_HEAP:
                return MemoryAllocator.HEAP;
            case OFF_HEAP:
                return MemoryAllocator.UNSAFE;
        }
        return null;
    }

    public long getExecutionMemoryUsageForTask(Long taskAttemptId) {
        return onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) + offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId);
    }

}

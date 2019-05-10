package io.mycat.memory;

import static io.mycat.memory.MemoryConf.EXECUTOR_MEMORY;

import java.util.Map;

public class UnifiedMemoryManager extends MemoryManager {
    private final long maxHeapMemory;

    public UnifiedMemoryManager(Map<MemoryConf, Object> conf, long maxHeapMemory, int numCores) {
        super(conf, numCores, maxHeapMemory);
        this.maxHeapMemory = maxHeapMemory;
    }

    @Override
    public long acquireExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
        ExecutionMemoryPool executionPool;
        switch (memoryMode) {
            case ON_HEAP:
                executionPool = onHeapExecutionMemoryPool;
                break;
            case OFF_HEAP:
                executionPool = offHeapExecutionMemoryPool;
                break;
            default:
                throw new RuntimeException("");
        }
        return executionPool.acquireMemory(numBytes, taskAttemptId);
    }

    // Set aside a fixed amount of memory for non-storage, non-execution purposes.
    // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
    // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
    // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
    private static final long RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024;

    public static UnifiedMemoryManager apply(Map<MemoryConf, Object> conf, int numCores) {
        long maxMemory = getMaxMemory(conf);
        return new UnifiedMemoryManager(
                conf,
                maxMemory,
                numCores);
    }

    /**
     * Return the total amount of memory shared between execution and storage, in bytes.
     */
    private static long getMaxMemory(Map<MemoryConf, Object> conf) {
        long systemMemory = Runtime.getRuntime().freeMemory();
        long reservedMemory = (Long) conf.getOrDefault(MemoryConf.RESERVED_MEMORY, RESERVED_SYSTEM_MEMORY_BYTES);//保留300mb;
        long minSystemMemory = (long) (reservedMemory * 1.5);
        if (systemMemory < minSystemMemory) {
            throw new IllegalArgumentException("System memory $systemMemory must " +
                    "be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
                    "option or ${config.DRIVER_MEMORY.key} in Spark configuration.");
        }
        // SPARK-12759 Check executor memory to fail fast if memory is insufficient
        Long aLong = (Long) conf.get(EXECUTOR_MEMORY);
        if (aLong != null) {
            if (aLong < minSystemMemory) {
                throw new IllegalArgumentException("Executor memory $executorMemory must be at least " +
                        "$minSystemMemory. Please increase executor memory using the " +
                        "--executor-memory option or ${config.EXECUTOR_MEMORY.key} in Spark configuration.");
            }
        }
        long usableMemory = systemMemory - reservedMemory;
        long memoryFraction = (Long) conf.get(MemoryConf.MEMORY_FRACTION);
        return (usableMemory * memoryFraction);
    }
}

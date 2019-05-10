package io.mycat.memory;

import java.util.HashMap;

public class ExecutionMemoryPool extends MemoryPool {
    final MemoryMode mode;
    final HashMap<Long, Long> memoryForTask = new HashMap<>();

    public ExecutionMemoryPool(Object lock, MemoryMode onHeap) {
        super(lock);
        this.mode = onHeap;
    }

    public Long getMemoryUsageForTask(Long taskAttemptId) {
        return memoryForTask.getOrDefault(taskAttemptId, 0L);
    }


    public Long acquireMemory(long numBytes, long taskAttemptId) {
        Long aLong = memoryForTask.get(taskAttemptId);
        if (aLong == null) {
            memoryForTask.put(taskAttemptId, 0L);
            lock.notifyAll();
        }
        while (true) {
            int numActiveTasks = memoryForTask.size();
            Long curMem = memoryForTask.get(taskAttemptId);
            long maxPoolSize = poolSize();
            long maxMemoryPerTask = maxPoolSize / numActiveTasks;
            long minMemoryPerTask = poolSize() / (2 * numActiveTasks);

            long maxToGrant = Math.min(numBytes, Math.max(0, maxMemoryPerTask - curMem));
            // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
            long toGrant = Math.min(maxToGrant, memoryFree());

            if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
                logWarning("TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free");
                try {
                    lock.wait();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                Long aLong1 = memoryForTask.get(taskAttemptId);
                memoryForTask.put(taskAttemptId, aLong1 + toGrant);
                return toGrant;
            }
        }
    }

    public Long releaseAllMemoryForTask(Long taskAttemptId) {
        synchronized (lock) {
            Long numBytesToFree = getMemoryUsageForTask(taskAttemptId);
            releaseMemory(numBytesToFree, taskAttemptId);
            return numBytesToFree;
        }
    }

    public void releaseMemory(Long numBytes, Long taskAttemptId) {
        synchronized (lock) {
            Long memoryToFree;
            Long curMem = memoryForTask.getOrDefault(taskAttemptId, 0L);
            if (curMem < numBytes) {
                logWarning("Internal error: release called on $numBytes bytes but task only has $curMem bytes "
                        + "of memory from the $poolName pool");
                memoryToFree = curMem;
            } else {
                memoryToFree = numBytes;
            }
            Long aLong = memoryForTask.get(taskAttemptId);
            if (aLong != null) {
                long l = aLong - memoryToFree;
                if (l <= 0) {
                    memoryForTask.remove(taskAttemptId);
                } else {
                    memoryForTask.put(taskAttemptId, l);
                }
            }
        }
        lock.notifyAll();
    }

    void logWarning(Object... args) {

    }

    @Override
    public long memoryUsed() {
        long sum = 0;
        for (Long value : memoryForTask.values()) {
            sum += value;
        }
        return sum;
    }
}

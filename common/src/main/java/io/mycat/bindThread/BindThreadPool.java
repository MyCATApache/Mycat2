/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.bindThread;

import io.mycat.MycatException;
import io.mycat.ScheduleUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class BindThreadPool<KEY extends BindThreadKey, PROCESS extends BindThread> {

    final ConcurrentHashMap<KEY, PROCESS> map = new ConcurrentHashMap<>();
    final ArrayBlockingQueue<PROCESS> idleList;
    final ArrayBlockingQueue<PengdingJob> pending;
    final Function<BindThreadPool, PROCESS> processFactory;
    final Consumer<Exception> exceptionHandler;
    final AtomicInteger threadCounter = new AtomicInteger(0);
    final int minThread;
    final int maxThread;
    final long waitTaskTimeout;
    final TimeUnit timeoutUnit;
    private final ExecutorService noBindingPool;
    private final ScheduledFuture<?> schedule;

    long lastPollTaskTime = System.currentTimeMillis();

    public boolean isBind(KEY key) {
        return map.containsKey(key);
    }

    public BindThreadPool(int maxPengdingLimit, long waitTaskTimeout,
                          TimeUnit timeoutUnit, int minThread, int maxThread,
                          Function<BindThreadPool, PROCESS> processFactory,
                          Consumer<Exception> exceptionHandler) {
        this.waitTaskTimeout = waitTaskTimeout;
        this.timeoutUnit = timeoutUnit;
        this.minThread = minThread;
        this.maxThread = maxThread + 1;
        this.idleList = new ArrayBlockingQueue<>(maxThread);
        this.pending = new ArrayBlockingQueue<>(
                maxPengdingLimit < 0 ? 65535 : maxPengdingLimit);
        this.processFactory = processFactory;
        this.exceptionHandler = exceptionHandler;
       this.schedule = ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
            try {
                pollTask();
            }catch (Exception e){
                exceptionHandler.accept(e);
            }
        }, 1, 1, TimeUnit.MILLISECONDS);
        //   , 1, 1, TimeUnit.MILLISECONDS
        this.noBindingPool = Executors.newFixedThreadPool(maxThread);
    }

    void pollTask() {
        PROCESS process = null;
        try {
            process = idleList.poll(0, this.timeoutUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (process != null) {
            idleList.add(process);
        }
        PengdingJob poll = null;
        try {
            while ((poll = pending.poll(0, this.timeoutUnit)) != null) {
                if (!poll.run()) {
                    break;
                }
            }
        } catch (Exception e) {
            exceptionHandler.accept(e);
            if (poll != null) {
                poll.getTask().onException(poll.getKey(), e);
            }
        }
        if (poll == null) {
            long now = System.currentTimeMillis();
            long currentTimeMillis = now - lastPollTaskTime;
            long l = TimeUnit.SECONDS.convert(currentTimeMillis, TimeUnit.MILLISECONDS);
            if (l > 60) {
                tryDecThread();
            }
        } else {
            lastPollTaskTime = System.currentTimeMillis();
        }
        /////////////////////////////////
        for (Map.Entry<KEY, PROCESS> entry : map.entrySet()) {
            KEY key = entry.getKey();
            if (!key.isRunning()) {
                map.remove(key);
                entry.getValue().close();
            }
        }


        ////////////////////////////////

        this.threadCounter.updateAndGet(operand -> this.map.size() + this.idleList.size());

    }


    boolean tryIncThreadCount() {
        return threadCounter.updateAndGet(operand -> {
            if (maxThread <= operand) {
                return maxThread;
            } else {
                return ++operand;
            }
        }) < maxThread;
    }

    public void run(KEY key, BindThreadCallback<KEY, PROCESS> task) {
            ScheduleUtil.TimerTask timerFuture = ScheduleUtil.getTimerFuture(new Closeable() {
                @Override
                public void close() throws IOException {
                    task.onException(key,new MycatException("task timeout"));
                }
            }, waitTaskTimeout, timeoutUnit);
                Future<?> submit = noBindingPool.submit(() -> {
                    try {
                        task.accept(key, null);
                    } catch (Exception e) {
                        task.onException(key, e);
                        exceptionHandler.accept(e);
                    } finally {
                        timerFuture.setFinished();
                        task.finallyAccept(key, null);

                    }
                });
    }

    public boolean runOnBinding(KEY key, BindThreadCallback<KEY, PROCESS> task) {
        PROCESS transactionThread = map.computeIfAbsent(key, new Function<KEY, PROCESS>() {
            @Override
            public PROCESS apply(KEY key) {
                PROCESS transactionThread = idleList.poll();
                if (transactionThread == null) {
                    if (tryIncThreadCount()) {
                        transactionThread = processFactory.apply(BindThreadPool.this);
                        transactionThread.start();
                    } else {
                        if (!pending.offer(createPengdingTask(key, task))) {
                            task.onException(key, new Exception("max pending job limit"));
                        }
                        return null;
                    }
                }
                return transactionThread;
            }
        });
        if (transactionThread == null) return false;
        transactionThread.run(key, task);
        return true;
    }

    private PengdingJob createPengdingTask(KEY key, BindThreadCallback task) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(task);
        return new PengdingJob() {
            @Override
            public boolean run() {
                return BindThreadPool.this.runOnBinding(key, task);
            }

            @Override
            public BindThreadKey getKey() {
                return key;
            }

            @Override
            public BindThreadCallback getTask() {
                return task;
            }
        };
    }

    public void tryDecThread() {
        if (map.size() + idleList.size() > minThread) {
            PROCESS poll = idleList.poll();
            if (poll != null) {
                poll.close();
            }
        }
    }

    interface PengdingJob {

        boolean run();

        BindThreadKey getKey();

        BindThreadCallback getTask();
    }
}
package io.mycat.runtime;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public enum ProxySwitch {
    INSTANCE;
    final AtomicBoolean running = new AtomicBoolean(true);

    public void stopIfNeed() {
        if (running.get()) {
            return;
        }
        while (!running.get()) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public void continueRunning() {
        running.set(true);
    }

    public boolean stopRunning() {
        return running.compareAndSet(true, false);
    }
}
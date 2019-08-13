package io.mycat.replica.heartbeat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HeartBeatStatus {

  protected final AtomicBoolean isChecking = new AtomicBoolean(false); //是否正在检查
  protected final AtomicInteger errorCount = new AtomicInteger(0); //错误计数
  private final long minSwitchTimeInterval; //配置最小切换时间
  protected int maxRetry = 3; //错误maxRetry设置为错误
  private long lastSwitchTime;//上次主从切换时间

  public HeartBeatStatus(int maxRetry, long minSwitchTimeInterval, boolean isChecking,
      long lastSwitchTime) {
    this.maxRetry = maxRetry;
    this.minSwitchTimeInterval = minSwitchTimeInterval;
    this.isChecking.set(isChecking);
    this.lastSwitchTime = lastSwitchTime;
  }

  public int getMaxRetry() {
    return maxRetry;
  }

  public void setMaxRetry(int maxRetry) {
    this.maxRetry = maxRetry;
  }

  public long getMinSwitchTimeInterval() {
    return minSwitchTimeInterval;
  }

  public boolean isChecking() {
    return isChecking.get();
  }

  public void setChecking(boolean checking) {
    isChecking.set(checking);
  }

  public boolean tryChecking() {
    return isChecking.compareAndSet(false, true);
  }

  public int getErrorCount() {
    return errorCount.get();
  }

  public void setErrorCount(int count) {
    errorCount.set(count);
  }

  public long getLastSwitchTime() {
    return lastSwitchTime;
  }

  public void setLastSwitchTime(long lastSwitchTime) {
    this.lastSwitchTime = lastSwitchTime;
  }

  public void incrementErrorCount() {
    this.errorCount.incrementAndGet();
  }
}
/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heartbeat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
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
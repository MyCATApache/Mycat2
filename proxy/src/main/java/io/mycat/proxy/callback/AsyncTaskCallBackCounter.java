/**
 * Copyright (C) <2021>  <jamie12221>
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
package io.mycat.proxy.callback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221
 *  date 2019-05-22 11:50
 **/
public class AsyncTaskCallBackCounter {

  final AtomicInteger counter;
  final AsyncTaskCallBack callBack;
  final AtomicInteger fail = new AtomicInteger(0);

  public AsyncTaskCallBackCounter(int counter,
      AsyncTaskCallBack callBack) {
    this.counter = new AtomicInteger(counter);
    this.callBack = callBack;
  }

  public void onCountSuccess() {
    if (counter.decrementAndGet() == 0) {
      callBack.onFinished(this, null, null);
    }
  }

  public void onCountFail() {
    fail.incrementAndGet();
    if (counter.decrementAndGet() == 0) {
      callBack.onFinished(this, null, null);
    }
  }

  @Override
  public String toString() {
    return "AsyncTaskCallBackCounter{" +
               "counter=" + counter +
               ", callBack=" + callBack +
               ", fail=" + fail +
               '}';
  }
}

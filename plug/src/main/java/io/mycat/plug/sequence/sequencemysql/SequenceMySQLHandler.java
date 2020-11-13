///**
// * Copyright (C) <2019>  <chen junwen>
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.plug.sequence.sequencemysql;
//
//import io.mycat.plug.sequence.SequenceCallback;
//import io.mycat.plug.sequence.SequenceHandler;
//
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * chenjunwen 2019.6.4
// *
// * 1.本类线程安全 2.异步IO操作,一旦获取多个序列号之后再回调,一次只有一个IO操作运行
// */
//public class SequenceMySQLHandler implements SequenceHandler {
//
//  private final AtomicLong currentValue = new AtomicLong(0);//@todo AtomicLongArray
//  private final AtomicLong maxSequenceValue = new AtomicLong(-1);
//  private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
//
//  @Override
//  public void nextId(SequenceCallback callback) {
//    boolean successFetched = false;
//    long curSeq;
//    synchronized (this) {
//      successFetched = isSuccessFetched();
//      {//消除cas
//        long value = currentValue.get() + 1;
//        currentValue.set(value);
//        curSeq = value;
//      }
//    }
//    if (successFetched) {
//      if (curSeq < maxSequenceValue.get()) {
//        callback.onSequence(curSeq);
//        return;
//      }
//    }
//    fetch(callback);
//  }
//
//
//  private boolean isSuccessFetched() {
//    return maxSequenceValue.get() > 0;
//  }
//
//  private void fetch(SequenceCallback callback) {
//    boolean fetching = maxSequenceValue.getAndSet(-1) == -1;
//    if (fetching) {
//      queue.add(() -> {
//        nextId(callback);
//      });
//      return;
//    }
//    pengding(() -> {
//      boolean success = false;
//      long currentValue = 0;
//      long maxSequenceValue = 0;
//      Exception exception = null;
//      if (success) {
//        synchronized (this) {
//          this.currentValue.set(currentValue + 1);
//          this.maxSequenceValue.set(maxSequenceValue);
//        }
//        callback.onSequence(currentValue);
//      } else {
//        callback.onException(exception, this, null);
//      }
//      pengding(() -> {
//        for (Runnable runnable : queue) {
//          pengding(runnable);
//        }
//      });
//    });
//  }
//
//  private void pengding(Runnable runnable) {
//
//  }
//}
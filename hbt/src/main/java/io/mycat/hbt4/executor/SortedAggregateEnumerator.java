/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.hbt4.executor;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;

import java.util.Comparator;
import java.util.NoSuchElementException;

class SortedAggregateEnumerator<TSource, TKey, TAccumulate, TResult>
      implements Enumerator<TResult> {
    private final Enumerable<TSource> enumerable;
    private final Function1<TSource, TKey> keySelector;
    private final Function0<TAccumulate> accumulatorInitializer;
    private final Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder;
    private final Function2<TKey, TAccumulate, TResult> resultSelector;
    private final Comparator<TKey> comparator;
    private boolean isInitialized;
    private boolean isLastMoveNextFalse;
    private TAccumulate curAccumulator;
    private Enumerator<TSource> enumerator;
    private TResult curResult;

    SortedAggregateEnumerator(
        Enumerable<TSource> enumerable,
        Function1<TSource, TKey> keySelector,
        Function0<TAccumulate> accumulatorInitializer,
        Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
        final Function2<TKey, TAccumulate, TResult> resultSelector,
        final Comparator<TKey> comparator) {
      this.enumerable = enumerable;
      this.keySelector = keySelector;
      this.accumulatorInitializer = accumulatorInitializer;
      this.accumulatorAdder = accumulatorAdder;
      this.resultSelector = resultSelector;
      this.comparator = comparator;
      isInitialized = false;
      curAccumulator = null;
      enumerator = enumerable.enumerator();
      curResult = null;
      isLastMoveNextFalse = false;
    }

    @Override public TResult current() {
      if (isLastMoveNextFalse) {
        throw new NoSuchElementException();
      }
      return curResult;
    }

    @Override public boolean moveNext() {
      if (!isInitialized) {
        isInitialized = true;
        // input is empty
        if (!enumerator.moveNext()) {
          isLastMoveNextFalse = true;
          return false;
        }
      } else if (curAccumulator == null) {
        // input has been exhausted.
        isLastMoveNextFalse = true;
        return false;
      }

      if (curAccumulator == null) {
        curAccumulator = accumulatorInitializer.apply();
      }

      // reset result because now it can move to next aggregated result.
      curResult = null;
      TSource o = enumerator.current();
      TKey prevKey = keySelector.apply(o);
      curAccumulator = accumulatorAdder.apply(curAccumulator, o);
      while (enumerator.moveNext()) {
        o = enumerator.current();
        TKey curKey = keySelector.apply(o);
        if (comparator.compare(prevKey, curKey) != 0) {
          // current key is different from previous key, get accumulated results and re-create
          // accumulator for current key.
          curResult = resultSelector.apply(prevKey, curAccumulator);
          curAccumulator = accumulatorInitializer.apply();
          break;
        }
        curAccumulator = accumulatorAdder.apply(curAccumulator, o);
        prevKey = curKey;
      }

      if (curResult == null) {
        // current key is the last key.
        curResult = resultSelector.apply(prevKey, curAccumulator);
        // no need to keep accumulator for the last key.
        curAccumulator = null;
      }

      return true;
    }

    @Override public void reset() {
      enumerator.reset();
      isInitialized = false;
      curResult = null;
      curAccumulator = null;
      isLastMoveNextFalse = false;
    }

    @Override public void close() {
      enumerator.close();
    }
  }
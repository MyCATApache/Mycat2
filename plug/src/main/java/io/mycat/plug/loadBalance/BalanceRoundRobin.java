/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.plug.loadBalance;


import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * roundRobin
 *
 */
public enum BalanceRoundRobin implements LoadBalanceStrategy{
  INSTANCE {
    private final ConcurrentMap<String, AtomicInteger> sequences = new ConcurrentHashMap<String, AtomicInteger>();
    @Override
    public LoadBalanceElement select(LoadBalanceInfo info, List<LoadBalanceElement> entityList) {
      if (null == entityList || entityList.isEmpty()) {
          return null;
      }
      int length = entityList.size();
      int maxWeight = Integer.MIN_VALUE;
      int minWeight = Integer.MAX_VALUE;
      int sumWeight = 0;
      int[] weightList = new int[entityList.size()]; //权重list
      for (int i = 0; i < length; i++) {
        LoadBalanceElement loadBalanceDataSource = entityList.get(i);
        int weight = loadBalanceDataSource.getWeight();
        maxWeight = Math.max(maxWeight, weight);
        minWeight = Math.max(minWeight, weight);
        if(weight > 0) {
          weightList[i] = weight;
          sumWeight += weight;
        } else {
          weightList[i] = 0;
        }
      }
      AtomicInteger sequence = sequences.get(info.getName());
      if(null == sequence) {
        sequences.putIfAbsent(info.getName(), new AtomicInteger());
        sequence = sequences.get(info.getName());
      }
      int nextSequence = sequence.incrementAndGet();
      if(sumWeight > 0 && minWeight < maxWeight) {
        nextSequence = nextSequence % sumWeight;
        int randomIndex = ThreadLocalRandom.current().nextInt(0, sumWeight);
        for(int i = 0 ; i < sumWeight; i++) {
          for(int index = 0 ; i < length; i ++) {
            if(weightList[index] > 0) {
              weightList[index] = weightList[index] - 1;
              nextSequence --;
              if(nextSequence == 0) {
                  return entityList.get(index);
              }
            }
          }
        }
      }
      return entityList.get(nextSequence % length);
    }
  }
}


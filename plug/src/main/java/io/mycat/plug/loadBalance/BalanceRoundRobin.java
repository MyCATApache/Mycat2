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
    public  LoadBalanceDataSource select(LoadBalanceInfo info, List<LoadBalanceDataSource> entityList) {
      if(null == entityList && entityList.size() == 0) {
          return null;
      }
      int length = entityList.size();
      int maxWeight = Integer.MIN_VALUE;
      int minWeight = Integer.MAX_VALUE;
      int sumWeight = 0;
      int[] weightList = new int[entityList.size()]; //权重list
      for (int i = 0; i < length; i++) {
        LoadBalanceDataSource loadBalanceDataSource = entityList.get(i);
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
        int randomIndex = ThreadLocalRandom.current().nextInt(sumWeight);
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
  };
}


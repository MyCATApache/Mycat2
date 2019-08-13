package io.mycat.replica;

import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.SessionCounter;
import java.util.concurrent.CopyOnWriteArraySet;

public class PhysicsInstanceImpl implements LoadBalanceElement, PhysicsInstance {

  final InstanceType type;
  final int index;
  final String name;
  final ReplicaDataSourceSelector selector;
  final int weight;
  final CopyOnWriteArraySet<SessionCounter> sessionCounters = new CopyOnWriteArraySet<>();
  volatile boolean alive;
  volatile boolean selectRead;

  public PhysicsInstanceImpl(int index, String name, InstanceType type, boolean alive,
      boolean selectRead,
      int weight, ReplicaDataSourceSelector selector) {
    this.type = type;
    this.index = index;
    this.name = name;
    this.alive = alive;
    this.selectRead = selectRead;
    this.weight = weight;
    this.selector = selector;
  }

  public boolean isAlive() {
    return alive;
  }

  public InstanceType getType() {
    return type;
  }

  public int getIndex() {
    return index;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean isMaster() {
    return selector.writeDataSource.contains(this);
  }

  @Override
  public boolean asSelectRead() {
    return isAlive() && selectRead;
  }

  @Override
  public int getSessionCounter() {
    int count = 0;
    for (SessionCounter sessionCounter : sessionCounters) {
      count += sessionCounter.getSessionCounter();
    }
    return count;
  }

  @Override
  public int getWeight() {
    return weight;
  }

  public void notifyChangeAlive(boolean alive) {
    this.alive = alive;
  }

  public void notifyChangeSelectRead(boolean readable) {
    this.selectRead = readable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PhysicsInstanceImpl instance = (PhysicsInstanceImpl) o;

    if (index != instance.index) {
      return false;
    }
    return name != null ? name.equals(instance.name) : instance.name == null;
  }

  @Override
  public int hashCode() {
    int result = index;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
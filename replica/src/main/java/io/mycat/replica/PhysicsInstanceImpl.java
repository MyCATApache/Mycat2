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
package io.mycat.replica;

import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.SessionCounter;

import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
public class PhysicsInstanceImpl implements LoadBalanceElement, PhysicsInstance {
  final InstanceType type;
  final String name;
  final ReplicaDataSourceSelector selector;
  final int weight;
  final CopyOnWriteArraySet<SessionCounter> sessionCounters = new CopyOnWriteArraySet<>();
  volatile boolean alive;
  volatile boolean selectRead;

  public PhysicsInstanceImpl(String name, InstanceType type, boolean alive,
                             boolean selectRead,
                             int weight, ReplicaDataSourceSelector selector) {
    this.type = type;
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
    return name != null ? name.equals(instance.name) : instance.name == null;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica;

import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.SessionCounter;
import lombok.ToString;

import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
@ToString
public class PhysicsInstanceImpl implements LoadBalanceElement, PhysicsInstance {
    final InstanceType type;
    final String name;
    final SessionCounter sessionCounter;
    final ReplicaDataSourceSelector selector;
    final int weight;
    volatile boolean alive;
    volatile boolean selectRead;

    public PhysicsInstanceImpl(String name, InstanceType type, boolean alive,
                               boolean selectRead,
                               int weight,
                               SessionCounter sessionCounter,
                               ReplicaDataSourceSelector selector) {
        this.type = type;
        this.name = name;
        this.alive = alive;
        this.selectRead = selectRead;
        this.weight = weight;
        this.sessionCounter = sessionCounter;
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
        switch (selector.getType()) {
            case SINGLE_NODE:
            case MHA:
            case MASTER_SLAVE:
                if (selector.writeDataSourceList.contains(this)) {
                    return selector.writeDataSourceList.size() == 1 || selector.writeDataSourceList.get(0) == this;
                }
                return false;
            case MGR:
            case GARELA_CLUSTER:
            case NONE:
            default:
                return selector.writeDataSourceList.contains(this);
        }
    }

    @Override
    public boolean isBackup() {
        switch (selector.getType()) {
            case SINGLE_NODE:
                return false;
            case MHA:
            case MASTER_SLAVE:
                if (selector.writeDataSourceList.contains(this)) {
                    return selector.writeDataSourceList.get(0) != this;
                }
                return false;
            case MGR:
                return false;
            case GARELA_CLUSTER:
            case NONE:
            default:
                return false;
        }
    }

    @Override
    public boolean asSelectRead() {
        return isAlive() && selectRead;
    }

    @Override
    public int getSessionCounter() {
        return sessionCounter.getSessionCounter();
    }


    @Override
    public int getWeight() {
        return weight;
    }

    public synchronized void notifyChangeAlive(boolean alive) {
        this.alive = alive;
    }

    public synchronized void notifyChangeSelectRead(boolean readable) {
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
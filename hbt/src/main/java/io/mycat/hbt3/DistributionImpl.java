package io.mycat.hbt3;

import io.mycat.DataNode;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
public class DistributionImpl extends Distribution {
    final List<DataNode> dataNodeList;
    final Type type;
    final boolean partial;


    public DistributionImpl(List<DataNode> dataNodeList,
                            boolean partial,
                            Type type) {
        this.partial = partial;
        this.type = type;
        if (dataNodeList.isEmpty()) {
            throw new AssertionError();
        }
        this.dataNodeList = dataNodeList;
    }

    @Override
    public Iterable<DataNode> getDataNodes(List<Object> params) {
        return dataNodeList;
    }

    @Override
    public List<DataNode> getDataNodes() {
        return dataNodeList;
    }

    @Override
    public boolean isSingle() {
        return dataNodeList.size() == 1;
    }

    @Override
    public boolean isBroadCast() {
        return false;
    }

    @Override
    public boolean isSharding() {
        return false;
    }

    @Override
    public boolean isPartial() {
        return partial;
    }

    @Override
    public Type type() {
        return this.type;
    }

    @Override
    public boolean isPhy() {
        return this.type == Type.PHY;
    }

    @Override
    public String toString() {
        return "{" +
                "dataNodeList=" + dataNodeList +
                ", type=" + type +
                ", partial=" + partial +
                '}';
    }
}
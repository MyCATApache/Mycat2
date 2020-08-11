package io.mycat.hbt3;

import io.mycat.DataNode;

import java.util.List;

public class DistributionImpl extends Distribution {
    final List<DataNode> dataNodeList;
    final String digest;
    final Type type;


    public DistributionImpl(List<DataNode> dataNodeList,
                            String digest,
    Type type) {
        this.type = type;
        if (dataNodeList.isEmpty()) {
            throw new AssertionError();
        }
        this.dataNodeList = dataNodeList;
        this.digest = digest;
    }

    @Override
    public List<DataNode> getDataNodes() {
        return dataNodeList;
    }

    @Override
    public   String digest() {
        return digest;
    }

    @Override
    public  boolean isSingle() {
        return dataNodeList.size() == 1;
    }

    @Override
    public  boolean isBroadCast() {
        return false;
    }

    @Override
    public  boolean isSharding() {
        return false;
    }

    @Override
    public  boolean isJoin() {
        return false;
    }

    @Override
    public Type type() {
        return   this.type;
    }

    @Override
    public boolean isPhy() {
        return this.type == Type.PHY;
    }

}
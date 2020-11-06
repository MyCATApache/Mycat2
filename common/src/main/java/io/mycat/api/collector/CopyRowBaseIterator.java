package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

public class CopyRowBaseIterator extends AbstractObjectRowIterator {
    @Override
    public MycatRowMetaData getMetaData() {
        return null;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public void close() {

    }
}
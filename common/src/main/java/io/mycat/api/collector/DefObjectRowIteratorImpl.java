package io.mycat.api.collector;

import io.mycat.beans.mycat.DefMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import lombok.AllArgsConstructor;

import java.util.Iterator;

@AllArgsConstructor
public class DefObjectRowIteratorImpl extends AbstractObjectRowIterator {
    final DefMycatRowMetaData mycatRowMetaData  ;
    final Iterator<Object[]> iterator ;
    @Override
    public MycatRowMetaData getMetaData() {
        return mycatRowMetaData;
    }

    @Override
    public boolean next() {
        if (this.iterator.hasNext()) {
            this.currentRow = this.iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() {

    }
}
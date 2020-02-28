package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaDataImpl;
import lombok.AllArgsConstructor;

import java.util.Iterator;

@AllArgsConstructor
public class DefObjectRowIteratorImpl extends AbstractObjectRowIterator {
    final MycatRowMetaDataImpl mycatRowMetaData  ;
    final Iterator<Object[]> iterator ;
    @Override
    public MycatRowMetaData metaData() {
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
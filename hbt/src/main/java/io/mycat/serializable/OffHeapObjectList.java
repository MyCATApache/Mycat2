package io.mycat.serializable;

import io.mycat.beans.mycat.MycatRowMetaData;

public interface OffHeapObjectList extends Iterable<Object[]>{

    public void addObjects(Object[] objects);

    public void finish();

    public void close();

}

package io.mycat.api.collector;

import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;

import java.util.Iterator;

public class MergeUpdateRowIterator extends UpdateRowIterator {
    private Iterator<UpdateRowIterator> iteratorIterator;

    public MergeUpdateRowIterator(Iterator<UpdateRowIterator> iteratorIterator) {
        super(0, 0);
        this.iteratorIterator = iteratorIterator;
    }

    @Override
    public boolean next() {
        if (!next) {
            next = true;
            while (iteratorIterator.hasNext()) {
                try (UpdateRowIterator next = iteratorIterator.next()) {
                    this.updateCount += next.getUpdateCount();
                    this.lastInsertId += next.getLastInsertId();
                }
            }
            return true;
        } else {
            return false;
        }
    }

    MycatUpdateResponse nextToMycatUpdateResponse(int serverstatus) {
        next();
        return new MycatUpdateResponseImpl((int) this.updateCount, this.lastInsertId, serverstatus);
    }
}
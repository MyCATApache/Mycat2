package io.mycat.api.collector;

import java.util.Iterator;

public class MergeUpdateRowIterator extends UpdateRowIteratorResponse {
    private Iterator<UpdateRowIteratorResponse> iteratorIterator;

    public MergeUpdateRowIterator(Iterator<UpdateRowIteratorResponse> iteratorIterator,int serverStatus) {
        super(0, 0, serverStatus);
        this.iteratorIterator = iteratorIterator;
    }

    @Override
    public boolean next() {
        if (!next) {
            next = true;
            while (iteratorIterator.hasNext()) {
                try (UpdateRowIteratorResponse next = iteratorIterator.next()) {
                    this.updateCount += next.getUpdateCount();
                    this.lastInsertId += next.getLastInsertId();
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
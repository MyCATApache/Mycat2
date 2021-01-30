package io.mycat.beans.resultset;

import static io.mycat.beans.resultset.MycatResultSetType.OBSERVER_RRESULTSET;

public interface MycatObserverResponse extends MycatResponse{
    @Override
    public default MycatResultSetType getType() {
        return OBSERVER_RRESULTSET;
    }
}

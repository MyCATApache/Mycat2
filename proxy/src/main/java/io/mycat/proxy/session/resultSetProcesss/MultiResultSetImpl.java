package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.session.MycatSession;

import java.util.Iterator;

public class MultiResultSetImpl extends MultiResultSet {

    public MultiResultSetImpl(Iterator<ResultSet> resultSetIterator) {
        super(resultSetIterator);
    }

    @Override
    public void onRowEndPacket(MycatSession mycat) {

    }
}

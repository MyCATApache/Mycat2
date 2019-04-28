package io.mycat.proxy.session.resultSetProcesss;

import io.mycat.proxy.session.MycatSession;

import java.util.Iterator;

public abstract class MultiResultSet {
    Iterator<ResultSet> resultSetIterator;
    ResultSet currentResultSet;
    boolean isFinished;
    public MultiResultSet(Iterator<ResultSet> resultSetIterator) {
        this.resultSetIterator = resultSetIterator;
    }

    public void load() {
        currentResultSet = resultSetIterator.next();
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void run(MycatSession mycat) {
        if (currentResultSet.hasFinished()) {
            currentResultSet.close();
            if (resultSetIterator.hasNext()) {
                currentResultSet = resultSetIterator.next();
            } else {
                onRowEndPacket(mycat);
                close();
                isFinished = true;
                return;
            }
        } else {
            currentResultSet.run(mycat);
        }
    }

    abstract public void onRowEndPacket(MycatSession mycat);

    public void close() {
        currentResultSet.close();
        while (resultSetIterator.hasNext()) {
            resultSetIterator.next().close();
        }
    }
}

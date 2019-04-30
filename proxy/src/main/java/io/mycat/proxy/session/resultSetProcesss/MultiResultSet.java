/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

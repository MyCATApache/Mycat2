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
package io.mycat.proxy.session;

import io.mycat.proxy.MainMySQLNIOHandler;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.BackendConCreateTask;
import io.mycat.replica.MySQLDatasource;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

public class MySQLSessionManager implements BackendSessionManager<MySQLClientSession, MySQLDatasource> {
    LinkedList<MySQLClientSession> allSessions = new LinkedList<>();
    HashMap<MySQLDatasource, LinkedList<MySQLClientSession>> idleDatasourcehMap = new HashMap<>();
    private int count = 0;

    @Override
    public Collection<MySQLClientSession> getAllSessions() {
        return Collections.unmodifiableCollection(allSessions);
    }

    @Override
    public int curSessionCount() {
        return count;
    }

    @Override
    public void removeSession(MySQLClientSession session) {
        allSessions.remove(session);
        LinkedList<MySQLClientSession> mySQLSessions = idleDatasourcehMap.get(session.getDatasource());
        mySQLSessions.remove(session);
        count--;
    }

    @Override
    public void getIdleSessionsOfKey(MySQLDatasource datasource, AsynTaskCallBack<MySQLClientSession> asynTaskCallBack) {
        if (!datasource.isAlive()) {
            asynTaskCallBack.finished(null, this, false, null, datasource.getName() + " is not alive!");
        } else {
            LinkedList<MySQLClientSession> mySQLSessions = this.idleDatasourcehMap.get(datasource);
            if (mySQLSessions == null || mySQLSessions.isEmpty()) {
                createSession(datasource, asynTaskCallBack);
            } else {
                MySQLClientSession mySQLSession = ThreadLocalRandom.current().nextBoolean() ? mySQLSessions.removeFirst() : mySQLSessions.removeLast();
                asynTaskCallBack.finished(mySQLSession, this, true, null, null);
            }
        }
    }

    @Override
    public void addIdleSession(MySQLClientSession session) {
        idleDatasourcehMap.compute(session.getDatasource(), (k, l) -> {
            if (l == null) {
                l = new LinkedList<>();
            }
            l.add(session);
            return l;
        });
    }

    @Override
    public void removeIdleSession(MySQLClientSession session) {
        LinkedList<MySQLClientSession> mySQLSessions = idleDatasourcehMap.get(session.getDatasource());
        mySQLSessions.remove(session);
    }

    @Override
    public void clearAndDestroyMySQLSession(MySQLDatasource dsMetaBean, String reason) {

    }

    @Override
    public void createSession(MySQLDatasource key, AsynTaskCallBack<MySQLClientSession> callBack) {
        try {
            BackendConCreateTask conCreateTask = new BackendConCreateTask(key, this,
                (MycatReactorThread) Thread.currentThread(),
                (session, sender, success, result, attr) -> {
                    if (success){
                        count++;
                        allSessions.add(session);
                        callBack.finished(session,sender,success,result,attr);
                    }else {
                        callBack.finished(session,sender,success,result,attr);
                    }
                });
        } catch (Exception e) {
            try {
                callBack.finished(null, this, false, null, e.getMessage());
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    @Override
    public NIOHandler<MySQLClientSession> getDefaultSessionHandler() {
        return MainMySQLNIOHandler.INSTANCE;
    }

}

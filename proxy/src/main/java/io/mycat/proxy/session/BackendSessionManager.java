package io.mycat.proxy.session;

import io.mycat.proxy.task.AsynTaskCallBack;

public interface BackendSessionManager<T extends Session,ARG> extends SessionManager<T> {
    public void getIdleSessionsOfKey(ARG key,AsynTaskCallBack<T> asynTaskCallBack);
    public void addIdleSession(T Session);
    public void removeIdleSession(T Session);
    public  void createSession(ARG key, AsynTaskCallBack<T> callBack);
    public  void clearAndDestroyMySQLSession(ARG dsMetaBean, String reason);
}

package io.mycat.proxy.task;

import io.mycat.proxy.session.Session;

public class AsynTaskFuture<T extends Session> implements AsynTaskCallBack<T> {
    AsynTaskCallBack<T> callBack;
    boolean hasResult = false;

    boolean hasCallback = false;


    T session;

    Object sender;

    boolean success;

    java.lang.Object result;

    Object errorMessage;

    public static <T extends Session> AsynTaskFuture<T> future() {
        return new AsynTaskFuture<>();
    }

    public static <T extends Session> AsynTaskFuture<T> future(AsynTaskCallBack<T> callback) {
        return new AsynTaskFuture<T>().setCallBack(callback);
    }

    public AsynTaskFuture<T> resetfuture() {
        this.callBack = null;
        return (AsynTaskFuture<T>) this;
    }


    public AsynTaskFuture<T> setCallBack(AsynTaskCallBack<T> callBack) {

        this.callBack = callBack;

        if (canCallback(hasResult)) {

            hasCallback = true;

            callBack.finished(session, sender, success, result, errorMessage);

        }

        return this;

    }

    private boolean canCallback(boolean hasResult) {

        return hasResult && !hasCallback;

    }

    @Override
    public void finished(T session, java.lang.Object sender, boolean success, java.lang.Object result, Object errorMessage) {
        hasResult = true;

        this.session = session;

        this.sender = sender;

        this.success = success;

        this.result = result;

        this.errorMessage = errorMessage;

        if (canCallback(callBack != null)) {

            hasCallback = true;

            callBack.finished(session, sender, success, result, errorMessage);

        }
    }
}

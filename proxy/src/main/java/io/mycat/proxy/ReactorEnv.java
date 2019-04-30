package io.mycat.proxy;

import io.mycat.proxy.session.Session;

/**
 * 在Reactor线程中传递局部变量
 * @author wuzhihui
 *
 */
public class ReactorEnv {
    public Session getCurSession() {
        return curSession;
    }

    public void setCurSession(Session curSession) {
        this.curSession = curSession;
    }

    private Session curSession;

}

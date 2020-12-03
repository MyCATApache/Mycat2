package io.mycat.configRaft;

import com.alipay.sofa.jraft.Closure;

public abstract class MycatTaskClosure implements Closure {
    private String cmd;
    private Object response;


    public String getCmd() {
        return cmd;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public Object getResponse() {
        return response;
    }

    public void setResponse(Object response) {
        this.response = response;
    }
}

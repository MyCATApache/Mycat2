package org.apache.calcite;

import io.mycat.Authenticator;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatUser;

public class MycatContext {
    public Object[] values;
    public static final ThreadLocal<MycatDataContext> CONTEXT = ThreadLocal.withInitial(() -> null);
    public Object getSessionVariable(String name){
        return CONTEXT.get().getVariable(false,name);
    }
    public Object getGlobalVariable(String name){
        return CONTEXT.get().getVariable(true,name);
    }
    public String getDatabase(){
        MycatDataContext mycatDataContext = CONTEXT.get();
        return mycatDataContext.getDefaultSchema();
    }
    public Long getLastInsertId(){
        return CONTEXT.get().getLastInsertId();
    }
    public Long getConnectionId(){
        return CONTEXT.get().getSessionId();
    }
    public Object getUserVariable(String name){
        return null;
    }
    public String getCurrentUser(){
        MycatUser user = CONTEXT.get().getUser();
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        return user.getUserName()+"@"+authenticator.getUserInfo(user.getUserName()).getIp();
    }

    public String getUser(){
        MycatUser user = CONTEXT.get().getUser();
        return user.getUserName()+"@"+user.getHost();
    }
}
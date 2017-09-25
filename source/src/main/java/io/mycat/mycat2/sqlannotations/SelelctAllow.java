package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Map;

/**
 * Created by jamie on 2017/9/24.
 */
public class SelelctAllow implements SQLAnnotation{
   Object args;
    public SelelctAllow() {
        if (isDebug)
            System.out.println("=>SelelctAllow 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        System.out.println("=>SelelctAllow 动态注解初始化");
        this.args=args;


    }

    @Override
    public Boolean apply(MycatSession context) {
        if (isDebug)
            System.out.println("=>SelelctAllow 动态注解被调用"+args.toString());
        return false;
    }
    @Override
    public String getMethod() {
        return null;
    }

    @Override
    public void setMethod(String method) {

    }

}

package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Map;

/**
 * Created by jamie on 2017/9/24.
 */
public class Blacklist implements SQLAnnotation{

    public Blacklist() {
        if (isDebug)
            System.out.println("=>Blacklist 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        System.out.println("=>Blacklist 动态注解初始化");


    }

    @Override
    public Boolean apply(MycatSession context) {
        System.out.println("=>Blacklist");
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

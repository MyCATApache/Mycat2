package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Map;

/**
 * Created by jamie on 2017/9/24.
 */
public class Whitehost  implements SQLAnnotation{

    public Whitehost() {
        if (isDebug)
            System.out.println("=>Whitehost 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        System.out.println("=>Whitehost 动态注解初始化");
    }

    @Override
    public Boolean apply(MycatSession context) {
        System.out.println("========================> Whitehost ");
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

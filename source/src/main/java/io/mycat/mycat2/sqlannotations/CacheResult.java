package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.sqlparser.BufferSQLContext;

import java.util.Map;

/**
 * Created by jamie on 2017/9/15.
 */
public class CacheResult implements SQLAnnotation<BufferSQLContext>{
    Map<String,String> args;
    public CacheResult() {
        if (isDebug)
        System.out.println("=>CacheResult 对象本身的构造 初始化");
    }

    @Override
    public void init(Map<String,String> args) {
        System.out.println("=>CacheResult 动态注解初始化");
        this.args=args;
        if (args != null)
        args.entrySet().stream().forEach((c)->System.out.format("param:%s,value:%s\n",c.getKey(),c.getValue()));


    }

    @Override
    public BufferSQLContext apply(BufferSQLContext context) {
        if (isDebug)
        System.out.println("=>CacheResult 动态注解被调用"+args.toString());
        return context;
    }
}

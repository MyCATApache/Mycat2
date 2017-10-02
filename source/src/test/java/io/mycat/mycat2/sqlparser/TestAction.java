package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;

/**
 * Created by jamie on 2017/10/1.
 */
public class TestAction extends SQLAnnotation {


    Object args;

    public TestAction() {
        System.out.println("=>TestAction 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        this.args = args;
        System.out.println(String.format("%s:%s:%s 动态注解初始化", this.getClass().getName(), getActionName(), this.args));
    }

    @Override
    public Boolean apply(MycatSession context) {
        System.out.println(String.format("%s:%s:%s 被调用", this.getClass().getName(), getActionName(), this.args));
        return Boolean.TRUE;
    }


    @Override
    public MySQLCommand getMySQLCommand() {
        return null;
    }
}
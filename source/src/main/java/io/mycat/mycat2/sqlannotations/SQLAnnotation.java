package io.mycat.mycat2.sqlannotations;

import java.util.function.Function;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.ActonFactory;

/**
 * Created by jamie on 2017/9/15.
 */
public abstract class SQLAnnotation implements Function<MycatSession, Boolean> {
    ActonFactory actonFactory;
    String actionName;
   abstract public void init(Object args);


    public String getActionName(){
        return actionName;
    }

    public void setActionName(String actionName){
        this.actionName=actionName;
    }

    abstract public MySQLCommand getMySQLCommand();
    
    /**
     * 默认的重复检查, 命令链会根据该方法，进行去重复操作。
     * 如果 需要有多个实例,可以返回不同的值。
     * @return
     */    
    public  long currentKey() {
		// 结果集缓存. 在责任链中 只允许出现一次，这里返回相同的值
		return this.getClass().getSimpleName().hashCode();
	}

    public ActonFactory getActonFactory() {
        return actonFactory;
    }

    public void setActonFactory(ActonFactory actonFactory) {
        this.actonFactory = actonFactory;
    }
}

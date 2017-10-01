package io.mycat.mycat2.sqlannotations;

import java.util.function.Function;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/15.
 */
public interface SQLAnnotation extends Function<MycatSession, Boolean> {

   abstract public void init(Object args);


    public String getMethod();

    public void setMethod(String method);
    
    public MySQLCommand getMySQLCommand();
    
    /**
     * 默认的重复检查, 命令链会根据该方法，进行去重复操作。
     * 如果 需要有多个实例,可以返回不同的值。
     * @return
     */    
    public default long currentKey() {
		// 结果集缓存. 在责任链中 只允许出现一次，这里返回相同的值
		return this.getClass().getSimpleName().hashCode();
	}

}

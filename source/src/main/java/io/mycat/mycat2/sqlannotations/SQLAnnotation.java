package io.mycat.mycat2.sqlannotations;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;

/**
 * Created by jamie on 2017/9/15.
 */
public abstract class SQLAnnotation {

    String actionName;
    
   private SQLAnnotationMeta sqlAnnoMeta;
       
   abstract public void init(Object args);


    public String getActionName(){
        return actionName;
    }

    public void setActionName(String actionName){
        this.actionName=actionName;
    }
    
    abstract public boolean  apply(MycatSession session,SQLAnnotationChain chain);
    
    /**
     * 默认的重复检查, 命令链会根据该方法，进行去重复操作。
     * 如果 需要有多个实例,可以返回不同的值。
     * @return
     */    
    public  long currentKey() {
		// 结果集缓存. 在责任链中 只允许出现一次，这里返回相同的值
		return this.getClass().getSimpleName().hashCode();
	}


	public SQLAnnotationMeta getSqlAnnoMeta() {
		return sqlAnnoMeta;
	}


	public void setSqlAnnoMeta(SQLAnnotationMeta sqlAnnoMeta) {
		this.sqlAnnoMeta = sqlAnnoMeta;
	}

}

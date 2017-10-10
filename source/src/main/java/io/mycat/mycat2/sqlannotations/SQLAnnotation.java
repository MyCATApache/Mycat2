package io.mycat.mycat2.sqlannotations;

import java.util.function.Function;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/15.
 */
public abstract class SQLAnnotation implements Function<MycatSession, Boolean> {

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
     * Ĭ�ϵ��ظ����, ����������ݸ÷���������ȥ�ظ�������
     * ��� ��Ҫ�ж��ʵ��,���Է��ز�ͬ��ֵ��
     * @return
     */    
    public  long currentKey() {
		// ���������. ���������� ֻ�������һ�Σ����ﷵ����ͬ��ֵ
		return this.getClass().getSimpleName().hashCode();
	}

}

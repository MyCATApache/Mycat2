package io.mycat.mycat2.cmds.interceptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;

public class SQLAnnotationChain {
	
	private MySQLCommand target;
	
	/**
	 * 本次匹配到的所有的动态注解，里面可能有重复的anno。但是是不同实例。可以根据自己的需要，选择去重复，或者不去重复。
	 */
	private List<SQLAnnotation> annontations = new ArrayList<>(30);
	
	/**
	 * queueMap 用于去重复
	 */
	private LinkedHashMap<Long,MySQLCommand> queueMap = new LinkedHashMap<>(20);
	
	/**
	 * 前置类，后置类，around 类  动态注解  顺序，实现了SQLCommand 的动态注解会出现在此列表中
	 *  如果没有实现  SQLCommand 的 annotations 不会出现在此列表中
	 *  最终的构建结果
	 */		
	private List<MySQLCommand> queue = new ArrayList<>(20);
	
	/**
	 * queue 列表当前索引值
	 */
	private int cmdIndex = 0;
	
	private String errMsg;
	
	public void setTarget(MySQLCommand target){
		this.target = target;
	}
	
	public void build(){
		queue = queueMap.values().stream().collect(Collectors.toList());
	}
	
	public void clear(){
		queue.clear();
		cmdIndex = 0;
		annontations.clear();
		queueMap.clear();
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}
	
	public void addCmdChain(SQLAnnotation sqlanno,MySQLCommand command){
		queueMap.put(sqlanno.currentKey(), command);
	}

	public MySQLCommand next() {
		if((queue.isEmpty())|| (cmdIndex >= queue.size())){
			cmdIndex = 0;
			return target;
		}
		return queue.get(cmdIndex++);
	}

//	@Override
//	public boolean hasNext() {
//		return (!queue.isEmpty())|| (++cmdIndex < queue.size());
//	}
}

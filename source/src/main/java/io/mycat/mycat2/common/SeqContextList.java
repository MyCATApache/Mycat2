package io.mycat.mycat2.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.mycat.mycat2.AbstractMySQLSession;

/**
 * 序列存放流程执行信息
 * 
 * @author liujun
 * 
 * @date 2014年4月3日
 * 
 */
public class SeqContextList {

	/**
	 * 用来存放流程的容器
	 */
	private List<ChainExecInf> linkedServ = new LinkedList<ChainExecInf>();

	/**
	 * 用来存放参数的集合
	 */
	private Map<String, Object> param = new HashMap<String, Object>();

	/**
	 * session对象信息
	 */
	private AbstractMySQLSession session;

	/**
	 * 添加流程代码
	 * 
	 * @param serviceExec
	 */
	public void addExec(ChainExecInf serviceExec) {
		this.linkedServ.add(serviceExec);
	}

	public void clear() {
		// 清空参数
		param.clear();
		// 清空流程容器
		linkedServ.clear();

	}

	/**
	 * 添加流程代码
	 * 
	 * @param serviceExec
	 *            [] 流程执行数组
	 */
	public void addExec(ChainExecInf[] serviceExec) {
		if (null != serviceExec) {
			for (int i = 0; i < serviceExec.length; i++) {
				this.linkedServ.add(serviceExec[i]);
			}
		}
	}

	public void putParam(String key, Object value) {
		this.param.put(key, value);
	}

	public Object getValue(String key) {
		return param.get(key);
	}

	public Map<String, Object> getParam() {
		return param;
	}

	public AbstractMySQLSession getSession() {
		return session;
	}

	public void setSession(AbstractMySQLSession session) {
		this.session = session;
	}

	/**
	 * 执行下一个流程代码
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean nextExec() throws Exception {

		if (null != linkedServ && linkedServ.size() > 0) {

			ChainExecInf servExec = linkedServ.remove(0);

			return servExec.invoke(this);
		} else {
			return true;
		}
	}

}

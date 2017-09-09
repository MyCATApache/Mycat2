package io.mycat.mycat2.beans;

import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.ProxyReactorThread;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * 保存后端MySQL连接的Map
 * 
 * @author wuzhihui
 *
 */
public class SessionMap {
	// key - reactor name
	private final HashMap<String, LinkedList<MySQLSession>> items = new HashMap<>();

	public List<MySQLSession> getReactorConnections(String reactor) {
		LinkedList<MySQLSession> cons = items.get(reactor);
		if (cons == null) {
			cons = new LinkedList<>();
			items.put(reactor, cons);
		}
		return cons;
	}

	public MySQLSession tryTakeCon(final String reactor) {
		final LinkedList<MySQLSession> conList = items.get(reactor);
		if (conList == null) {
			items.put(reactor, new LinkedList<>());
		}
		if (!conList.isEmpty()) {
			return conList.removeLast();
		}
//		for (LinkedList<MySQLSession> curConList : items.values()) {
//			if (!curConList.isEmpty()) {
//				return curConList.removeLast();
//			}
//		}
		return null;
	}

	public Collection<LinkedList<MySQLSession>> getAllCons() {
		return items.values();
	}

	public void returnCon(String reactor, MySQLSession c) {
		getReactorConnections(reactor).add(c);
	}

}

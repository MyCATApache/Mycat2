package io.mycat.mycat2.beans;

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
public class ConMap {
	// key -schema
	private final HashMap<String, LinkedList<BackConnection>> items = new HashMap<>();

	public List<BackConnection> getSchemaConnections(String schema) {
		LinkedList<BackConnection> cons = items.get(schema);
		if (cons == null) {
			cons = new LinkedList<>();
			items.put(schema, cons);

		}
		return cons;
	}

	public BackConnection tryTakeCon(final String schema) {
		final LinkedList<BackConnection> conList = items.get(schema);
		if (!conList.isEmpty()) {
			return conList.removeLast();
		}
		for (LinkedList<BackConnection> curConList : items.values()) {
			if (!curConList.isEmpty()) {
				return curConList.removeLast();
			}
		}
		return null;
	}

	public Collection<LinkedList<BackConnection>> getAllCons() {
		return items.values();
	}

	public void returnCon(BackConnection c) {
		getSchemaConnections(c.schema).add(c);

	}

}

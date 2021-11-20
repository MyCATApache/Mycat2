package io.mycat.prototypeserver.mysql;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class CreateMySQLSQLSet {

	public static Map<String, String> getFieldValue(Class argClass)  {
		Field[] fields = argClass.getFields();

		Map<String,String> map = new HashMap<>();
		for (Field field : fields) {
			if(field.getType() == String.class){
				String name = field.getName();
				try{
					String o = (String)field.get(null);
					map.put(name,o);
				}catch (Exception e){

				}

			}
		}
		return map;
	}
}

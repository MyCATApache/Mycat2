package io.mycat.mycat2.sqlannotations;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.ConditionUtil;

/**
 * Created by jamie on 2017/9/24.
 */
public class Blacklist extends SQLAnnotation{
	
	List<SQLAnnotation> sqlAnnotations;
	
	private static final Logger logger = LoggerFactory.getLogger(Blacklist.class);

    public Blacklist() {
    }

    @Override
    public void init(Object args) {
    	List<Map<String, String>> list = (List) args;
    	sqlAnnotations = list.stream().map((i) -> 
    					getActonFactory()
    					.getActionByActionName(ConditionUtil.mappingKey(i),ConditionUtil.mappingValue(i)))
    					.collect(Collectors.toList());
    }

    @Override
    public boolean apply(MycatSession context,SQLAnnotationChain chain) {
    	for (SQLAnnotation f : sqlAnnotations) {
    		 if (!f.apply(context,chain)) {
    			 return false;
    		 }
    	}
        return true;
    }
}
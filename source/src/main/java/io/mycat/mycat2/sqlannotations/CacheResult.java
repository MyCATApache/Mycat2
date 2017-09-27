package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/15.
 */
public class CacheResult implements SQLAnnotation {
	
	private static final Logger logger = LoggerFactory.getLogger(CacheResult.class);

    public CacheResult() {
        logger.debug("=>CacheResult 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        logger.debug("=>CacheResult 动态注解初始化");
    }

    @Override
    public Boolean apply(MycatSession context) {
    	logger.debug("=>CacheResult");
        return Boolean.TRUE;
    }

    @Override
    public String getMethod() {
        return null;
    }

    @Override
    public void setMethod(String method) {

    }

}

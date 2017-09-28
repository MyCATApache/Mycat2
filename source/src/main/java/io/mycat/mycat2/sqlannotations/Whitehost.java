package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/24.
 */
public class Whitehost  implements SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(Whitehost.class);

    public Whitehost() {
    	logger.debug("=>Whitehost 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
    	logger.debug("=>Whitehost 动态注解初始化");
    }

    @Override
    public Boolean apply(MycatSession context) {
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

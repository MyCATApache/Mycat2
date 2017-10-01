package io.mycat.mycat2.sqlannotations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;

/**
 * Created by jamie on 2017/9/24.
 */
public class TableCheck extends SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(TableCheck.class);
	
    public TableCheck() {
        logger.debug("=>TableCheck 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        logger.debug("=>TableCheck 动态注解初始化");
    }

    @Override
    public Boolean apply(MycatSession context) {
        return Boolean.TRUE;
    }



	@Override
	public MySQLCommand getMySQLCommand() {
		return null;
	}

}

package io.mycat.mycat2.sqlannotations.blackList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.interceptor.BlockSqlCmd;
import io.mycat.mycat2.cmds.interceptor.SQLAnnotationChain;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * Created by yanjunli on 2017/9/24.
 */
public class SelelctAllow extends SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(SelelctAllow.class);
		
    Object args;
    public SelelctAllow() {
    	logger.debug("=>SelelctAllow 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        logger.debug("=>SelelctAllow 动态注解初始化。 "+args);
        this.args=args;
    }

    @Override
    public boolean apply(MycatSession context,SQLAnnotationChain chain) {
    	if(!(boolean)args&&
    			((BufferSQLContext.SELECT_SQL == context.sqlContext.getSQLType())
    			||(BufferSQLContext.SELECT_INTO_SQL == context.sqlContext.getSQLType()))
    			||(BufferSQLContext.SELECT_FOR_UPDATE_SQL == context.sqlContext.getSQLType())){
    		
    		chain.setErrMsg("select  not allow ");
    		chain.addCmdChain(this,BlockSqlCmd.INSTANCE);
    		return false;
    	}
        return true;
    }
}

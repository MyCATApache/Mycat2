package io.mycat.mycat2.sqlannotations.blackList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.BlockSqlCmd;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * Created by yanjunli on 2017/9/24.
 */
public class AlterAllow implements SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(AlterAllow.class);
	
    Object args;
    public AlterAllow() {
    	logger.debug("=>AlterAllow 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        logger.debug("=>AlterAllow 动态注解初始化。 "+args);
        this.args=args;
    }

    @Override
    public Boolean apply(MycatSession context) {
    	if(!(boolean)args&&
    			(BufferSQLContext.ALTER_SQL == context.sqlContext.getSQLType())){
    		
    		context.getCmdChain().setErrMsg("alter not allow ");
    		context.getCmdChain().addCmdChain(this,BlockSqlCmd.INSTANCE);
    		return Boolean.FALSE;
    	}
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

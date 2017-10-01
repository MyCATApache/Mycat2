package io.mycat.mycat2.sqlannotations.blackList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.cmds.BlockSqlCmd;
import io.mycat.mycat2.sqlannotations.SQLAnnotation;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * Created by yanjunli on 2017/9/24.
 */
public class TruncateAllow extends SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(TruncateAllow.class);
	
	private static final MySQLCommand command = BlockSqlCmd.INSTANCE;
	
    Object args;
    public TruncateAllow() {
    	logger.debug("=>TruncateAllow 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
        logger.debug("=>TruncateAllow 动态注解初始化。 "+args);
        this.args=args;
    }

    @Override
    public Boolean apply(MycatSession context) {
    	if(!(boolean)args&&
    			(BufferSQLContext.TRUNCATE_SQL == context.sqlContext.getSQLType())){
    		
    		context.getCmdChain().setErrMsg("truncate not allow ");
    		context.getCmdChain().addCmdChain(this);
    		return Boolean.FALSE;
    	}
        return Boolean.TRUE;
    }


	@Override
	public MySQLCommand getMySQLCommand() {
		return command;
	}

}

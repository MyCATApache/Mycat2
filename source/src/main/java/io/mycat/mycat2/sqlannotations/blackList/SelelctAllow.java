package io.mycat.mycat2.sqlannotations.blackList;

import javafx.util.Pair;
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
public class SelelctAllow extends SQLAnnotation{
	
	private static final Logger logger = LoggerFactory.getLogger(SelelctAllow.class);
	
	private static final MySQLCommand command = BlockSqlCmd.INSTANCE;
		
    Object args;
    public SelelctAllow() {
    	logger.debug("=>SelelctAllow 对象本身的构造 初始化");
    }

    @Override
    public void init(Object args) {
    	if (args instanceof String) {
    		args =Boolean.valueOf((String) args);
		}
        logger.debug("=>SelelctAllow 动态注解初始化。 "+args);
        this.args=args;
    }

    @Override
    public Boolean apply(MycatSession context) {
    	if(!(boolean)args&&
    			((BufferSQLContext.SELECT_SQL == context.sqlContext.getSQLType())
    			||(BufferSQLContext.SELECT_INTO_SQL == context.sqlContext.getSQLType()))
    			||(BufferSQLContext.SELECT_FOR_UPDATE_SQL == context.sqlContext.getSQLType())){
    		
    		context.getCmdChain().setErrMsg("select  not allow ");
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

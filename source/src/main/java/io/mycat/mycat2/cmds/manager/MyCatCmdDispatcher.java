package io.mycat.mycat2.cmds.manager;

import java.util.ArrayList;
import java.util.List;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.manager.mycatswitch.MycatSwitchCmdProcessor;
import io.mycat.mycat2.cmds.manager.show.MycatShowCmdProcessor;
import io.mycat.mycat2.sqlparser.BufferSQLContext;

/**
 * mycat 命令调度器
 * @author yanjunli
 *
 */
public class MyCatCmdDispatcher{
	
	private static class LazyHolder {    
	     private static final MyCatCmdDispatcher INSTANCE = new MyCatCmdDispatcher();    
	}
	
	public static final MyCatCmdDispatcher getInstance() {    
       return LazyHolder.INSTANCE;    
    } 
		
	private static List<MycatCmdProcssor> processorlist = new ArrayList<>();
	
	static{
		processorlist.add(MycatShowCmdProcessor.getInstance());
		processorlist.add(MycatSwitchCmdProcessor.getInstance());
	}
	
	public MySQLCommand getMycatCommand(BufferSQLContext sqlContext){
		String sql = sqlContext.getRealSQL(0);
		
		ParseContext context = new ParseContext(sql);

		return processorlist
						.stream()
						.filter(f->f.apply(context))
						.findFirst()
						.orElse(NotSupportCmdProcessor.INSTANCE)
						.getCommand(context);
	}
}

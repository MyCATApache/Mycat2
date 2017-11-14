package io.mycat.mycat2.cmds.manager;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.NotSupportCmd;
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
	
	public final static String splitStr = "(?!^[\\s]*)\\s+(?![\\s]*$)";

	private static Map<String,MycatCmdProcssor> processorMap = new HashMap<>();
	
	static{
		processorMap.put("SHOW", MycatShowCmdProcessor.getInstance());
		processorMap.put("SWITCH",MycatSwitchCmdProcessor.getInstance());
	}
	
	public MySQLCommand getMycatCommand(BufferSQLContext sqlContext){
		String[] tokens = sqlContext.getRealSQL(0).split(splitStr);
		
		int level = 1;		

		if(tokens.length <(level+1)){
			return NotSupportCmd.INSTANCE;
		}
		
		ParseContext context = new ParseContext(tokens);
		
		MycatCmdProcssor processor = processorMap.get(tokens[level].trim().toUpperCase());
		
		if(processor==null){
			return NotSupportCmd.INSTANCE;
		}
		return processor.getCommand(context,level+1);
	}
}

package io.mycat.mycat2.cmds.manager;

import java.util.HashMap;
import java.util.Map;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.NotSupportCmd;

public abstract class MycatCmdProcssor{
	
	protected static Map<String,MySQLCommand> cmdMaps = new HashMap<>();
	
	public MySQLCommand getCommand(ParseContext context,int level) {
		
		if(context.tokens.length <level){
			return NotSupportCmd.INSTANCE;
		}
		
		MySQLCommand processor = cmdMaps.get(context.tokens[level].trim().toUpperCase());
		
		if(processor==null){
			return NotSupportCmd.INSTANCE;
		}
		
		return processor;
	}

}

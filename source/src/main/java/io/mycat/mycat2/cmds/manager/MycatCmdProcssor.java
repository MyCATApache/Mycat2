package io.mycat.mycat2.cmds.manager;

import java.util.function.Function;

import io.mycat.mycat2.MySQLCommand;

public interface MycatCmdProcssor extends Function<ParseContext,Boolean>{
	
	MySQLCommand getCommand(ParseContext context);

}

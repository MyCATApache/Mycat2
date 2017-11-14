package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.NotSupportCmd;

/**
 * 
 * @author yanjunli
 *
 */
public class NotSupportCmdProcessor implements MycatCmdProcssor {
	
	public static final NotSupportCmdProcessor INSTANCE = new NotSupportCmdProcessor();
	
	private NotSupportCmdProcessor(){}

	@Override
	public Boolean apply(ParseContext t) {
		return Boolean.FALSE;
	}

	@Override
	public MySQLCommand getCommand(ParseContext context) {
		return NotSupportCmd.INSTANCE;
	}


}

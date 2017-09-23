package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MycatSession;

/**
 * 
 * @author yanjunli
 *
 */
public interface CmdStrategy {
	
	MySQLCommand getMyCommand(MycatSession session);

}

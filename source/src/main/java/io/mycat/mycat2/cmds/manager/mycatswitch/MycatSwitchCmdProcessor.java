package io.mycat.mycat2.cmds.manager.mycatswitch;

import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;

/**
 * mycat switch 命令
 * @author yanjunli
 *
 */
public class MycatSwitchCmdProcessor extends MycatCmdProcssor {
	
	private static class LazyHolder {    
	     private static final MycatSwitchCmdProcessor INSTANCE = new MycatSwitchCmdProcessor();    
	}
	
	public static final MycatSwitchCmdProcessor getInstance() {    
       return LazyHolder.INSTANCE;    
   }
	
	static{
		cmdMaps.put("REPL",MycatSwitchReplCmd.INSTANCE);
	}
}

package io.mycat.mycat2.cmds.manager.show;

import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;

/**
 *  mycat show 命令
 * @author yanjunli
 *
 */
public class MycatShowCmdProcessor extends MycatCmdProcssor {
	
	private static class LazyHolder {    
	     private static final MycatShowCmdProcessor INSTANCE = new MycatShowCmdProcessor();    
	}
	
	public static final MycatShowCmdProcessor getInstance() {    
        return LazyHolder.INSTANCE;    
    }
	
	//新命令在这里注册
	static{
		cmdMaps.put("HELP",MycatShowHelpCmd.INSTANCE);
		cmdMaps.put("CONFIGS",MycatShowConfigsCmd.INSTANCE);
		cmdMaps.put("SESSIONS",MycatShowSessionsCmd.INSTANCE);
		cmdMaps.put("PROCESSLIST",MycatShowSessionsCmd.INSTANCE);
		
		descMaps.put("HELP","显示帮助信息");
		descMaps.put("CONFIGS","显示配置信息");
		descMaps.put("SESSIONS","显示当前连接进程信息");
		descMaps.put("PROCESSLIST","显示当前连接进程信息");
	}
}

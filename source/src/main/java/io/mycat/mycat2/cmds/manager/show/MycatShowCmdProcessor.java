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
		cmdMaps.put("CONFIGS",MycatShowConfigsCmd.INSTANCE);
	}
}

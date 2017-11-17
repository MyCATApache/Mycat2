package io.mycat.mycat2.cmds.manager.mycatswitch;

import java.util.HashMap;
import java.util.Map;

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
	
	private static Map<String,String> descMaps = new HashMap<>();
	
	static{
		cmdMaps.put("REPL",MycatSwitchReplCmd.INSTANCE);
		descMaps.put("REPL","设置主从切换");
	}
	
	@Override
	public Map<String,String> getDescMaps(){
		return descMaps;
	}
}

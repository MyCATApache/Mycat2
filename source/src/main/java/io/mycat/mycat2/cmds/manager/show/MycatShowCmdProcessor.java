package io.mycat.mycat2.cmds.manager.show;

import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;

/**
 * mycat show 命令
 * 
 * @author yanjunli
 *
 */
public class MycatShowCmdProcessor extends MycatCmdProcssor {
	public static final MycatShowCmdProcessor INSTANCE = new MycatShowCmdProcessor();

	private MycatShowCmdProcessor() {
		init();
	}

    @Override
    protected void init() {
        cmdMaps.put("HELP", MycatShowHelpCmd.INSTANCE);
        cmdMaps.put("CONFIGS", MycatShowConfigsCmd.INSTANCE);
        cmdMaps.put("SESSIONS", MycatShowSessionsCmd.INSTANCE);
        cmdMaps.put("PROCESSLIST", MycatShowSessionsCmd.INSTANCE);
        cmdMaps.put("THREADPOOL", MycatShowThreadPool.INSTANCE);
        
        descMaps.put("HELP", "show help information");
        descMaps.put("CONFIGS", "show config information");
        descMaps.put("SESSIONS", "same as show processlist and deprecated");
        descMaps.put("PROCESSLIST", "show current process information");
        descMaps.put("THREADPOOL", "Report threadPool status");
    }
}

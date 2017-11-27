package io.mycat.mycat2.cmds.manager.mycatswitch;

import io.mycat.mycat2.cmds.manager.MycatCmdProcssor;

/**
 * mycat switch 命令
 * 
 * @author yanjunli
 *
 */
public class MycatSwitchCmdProcessor extends MycatCmdProcssor {
    public static final MycatSwitchCmdProcessor INSTANCE = new MycatSwitchCmdProcessor();

    private MycatSwitchCmdProcessor() {
        init();
    }

    @Override
    protected void init() {
        cmdMaps.put("REPL", MycatSwitchReplCmd.INSTANCE);

        descMaps.put("REPL", "set replication switch");
    }
}

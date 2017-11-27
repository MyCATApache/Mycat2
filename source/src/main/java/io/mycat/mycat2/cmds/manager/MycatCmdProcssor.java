package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.NotSupportCmd;

import java.util.HashMap;
import java.util.Map;

public abstract class MycatCmdProcssor {
    protected Map<String, MySQLCommand> cmdMaps = new HashMap<>();
    protected Map<String, String> descMaps = new HashMap<>();

    public MySQLCommand getCommand(ParseContext context, int level) {
        if (context.tokens.length < level) {
            return NotSupportCmd.INSTANCE;
        }

        MySQLCommand processor = cmdMaps.get(context.tokens[level].trim().toUpperCase());

        if (processor == null) {
            return NotSupportCmd.INSTANCE;
        }

        return processor;
    }

    // 新命令在这里注册
    protected abstract void init();

    public Map<String, String> getDescMaps() {
        return descMaps;
    }
}

package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.NotSupportCmd;
import io.mycat.mycat2.cmds.manager.mycatswitch.MycatSwitchCmdProcessor;
import io.mycat.mycat2.cmds.manager.show.MycatShowCmdProcessor;
import io.mycat.mycat2.sqlparser.BufferSQLContext;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * mycat 命令调度器
 * 
 * @author yanjunli
 *
 */
public class MyCatCmdDispatcher {
	public static final MyCatCmdDispatcher INSTANCE = new MyCatCmdDispatcher();

	private MyCatCmdDispatcher() {
		init();
	}

    public final static String splitStr = " "; // "(?!^[\\s]*)\\s+(?![\\s]*$)";

    private Map<String, MycatCmdProcssor> processorMap = new HashMap();

    private void init() {
		processorMap.put("SHOW", MycatShowCmdProcessor.INSTANCE);
		processorMap.put("SWITCH", MycatSwitchCmdProcessor.INSTANCE);
    }

    public MySQLCommand getMycatCommand(BufferSQLContext sqlContext) {
        String[] tokens = StringUtils.split(sqlContext.getRealSQL(0), splitStr);

        int level = 1;

        if (tokens.length < (level + 1)) {
            return NotSupportCmd.INSTANCE;
        }

        ParseContext context = new ParseContext(tokens);

        MycatCmdProcssor processor = processorMap.get(tokens[level].trim().toUpperCase());

        if (processor == null) {
            return NotSupportCmd.INSTANCE;
        }
        return processor.getCommand(context, level + 1);
    }

    public Map<String, MycatCmdProcssor> getProcessorMap() {
        return processorMap;
    }
}

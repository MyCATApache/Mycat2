package io.mycat.mycat2.cmds.manager;

import io.mycat.mycat2.cmds.NotSupportCmd;

/**
 * 
 * @author yanjunli
 *
 */
public class NotSupportCmdHolder extends NotSupportCmd implements MycatCmdHolder {
	
	public static final NotSupportCmdHolder INSTANCE = new NotSupportCmdHolder();
	
	private NotSupportCmdHolder(){}

	@Override
	public Boolean apply(ParseContext t) {
		return Boolean.FALSE;
	}

}

package io.mycat.mycat2.cmds;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.cmds.manager.MycatShowConfigsCmd;
import io.mycat.mycat2.cmds.manager.MycatShowHelpCmd;
import io.mycat.mycat2.cmds.manager.MycatShowSessionsCmd;
import io.mycat.mycat2.sqlparser.MatchMethodGenerator;

import java.util.HashMap;
import java.util.Map;

public class MycatCmds {
	protected Map<Byte, MySQLCommand> MYCATCOMMANDMAP = new HashMap<>();
	protected Map<Long, Byte> HASHCMDMAP = new HashMap<>();
	protected Map<String, String> MYCATDESCMAP = new HashMap<>();
	
	public static final MycatCmds INSTANCE = new MycatCmds();
	
	public MycatCmds(){
		addCMD("help",45,MycatShowHelpCmd.INSTANCE,"显示帮助信息");
		addCMD("configs",46,MycatShowConfigsCmd.INSTANCE,"显示配置信息");
		addCMD("sessions",47,MycatShowSessionsCmd.INSTANCE,"显示当前连接进程信息");
		addCMD("processlist",48,MycatShowSessionsCmd.INSTANCE,"显示当前连接进程信息");
	}
	
	public Map<Byte, MySQLCommand> getCmdMap(){
		return MYCATCOMMANDMAP;
	}
	
	public Map<String, String> getCmdDescMap(){
		return MYCATDESCMAP;
	}	
	public void addCMD(String s,int cmd, MySQLCommand sqlcmd,String memo){
		MYCATCOMMANDMAP.put((byte)cmd, sqlcmd);
		HASHCMDMAP.put(MatchMethodGenerator.genHash(s.toCharArray()), (byte)cmd);
		MYCATDESCMAP.put(s, memo);
	}
	
	public Byte getMycatHashCmd(long longHash){
		  return HASHCMDMAP.get(longHash);		
	}
}

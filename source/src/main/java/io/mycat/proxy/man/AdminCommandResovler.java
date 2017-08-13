package io.mycat.proxy.man;

import java.util.HashMap;

import io.mycat.proxy.man.cmds.ClusterJoinPacketCommand;
import io.mycat.proxy.man.cmds.DefaultRefusedCmd;
import io.mycat.proxy.man.cmds.NodeRegInfoPacketCommand;
import io.mycat.proxy.man.packet.FailedPacket;

/**
 * 返回管理报文对应的处理命令
 * 
 * @author wuzhihui
 *
 */
public class AdminCommandResovler {
	
	private HashMap<Byte, AdminCommand> adminCommandMap = new HashMap<>();
	private AdminCommand defaultCmd=new DefaultRefusedCmd();

	
	public AdminCommandResovler()
	{
		initCmdMap();
	}
	protected void initCmdMap()
	{
		adminCommandMap.put(FailedPacket.PKG_NODE_REG, new NodeRegInfoPacketCommand());
		ClusterJoinPacketCommand joinCommand=new ClusterJoinPacketCommand();
		adminCommandMap.put(FailedPacket.PKG_JOIN_REQ_ClUSTER, joinCommand);
		adminCommandMap.put(FailedPacket.PKG_JOIN_NOTIFY_ClUSTER, joinCommand);
		adminCommandMap.put(FailedPacket.PKG_JOIN_ACK_ClUSTER, joinCommand);
	}
	public AdminCommand resolveCommand(byte pkgType) {
		AdminCommand cmd = this.adminCommandMap.get(pkgType);
		return (cmd != null) ? cmd : defaultCmd;
	}

	public AdminCommand getDefaultCmd() {
		return defaultCmd;
	}

	public void setDefaultCmd(AdminCommand defaultCmd) {
		this.defaultCmd = defaultCmd;
	}
	public HashMap<Byte, AdminCommand> getAdminCommandMap() {
		return adminCommandMap;
	}
	
	
}

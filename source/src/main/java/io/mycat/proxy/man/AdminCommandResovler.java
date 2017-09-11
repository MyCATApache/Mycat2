package io.mycat.proxy.man;

import java.util.HashMap;

import io.mycat.proxy.man.cmds.ClusterJoinPacketCommand;
import io.mycat.proxy.man.cmds.ConfigPacketCommand;
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
		adminCommandMap.put(ManagePacket.PKG_NODE_REG, new NodeRegInfoPacketCommand());
		ClusterJoinPacketCommand joinCommand = new ClusterJoinPacketCommand();
		adminCommandMap.put(ManagePacket.PKG_JOIN_REQ_ClUSTER, joinCommand);
		adminCommandMap.put(ManagePacket.PKG_JOIN_NOTIFY_ClUSTER, joinCommand);
		adminCommandMap.put(ManagePacket.PKG_JOIN_ACK_ClUSTER, joinCommand);
		ConfigPacketCommand configCommand = new ConfigPacketCommand();
		adminCommandMap.put(ManagePacket.PKG_CONFIG_VERSION_REQ, configCommand);
		adminCommandMap.put(ManagePacket.PKG_CONFIG_VERSION_RES, configCommand);
		adminCommandMap.put(ManagePacket.PKG_CONFIG_REQ, configCommand);
		adminCommandMap.put(ManagePacket.PKG_CONFIG_RES, configCommand);
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

package io.mycat.proxy.man.cmds;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;

/**
 * Server端用来处理NodeRegInfoPacket类型的报文，
 * 
 * @author wuzhihui
 *
 */
public class NodeRegInfoPacketCommand implements AdminCommand {
	protected static Logger logger = LoggerFactory.getLogger(NodeRegInfoPacketCommand.class);

	@Override
	public void handlerPkg(AdminSession session,byte cmdType) throws IOException {
		NodeRegInfoPacket pkg = new NodeRegInfoPacket();
		pkg.resolve(session.readingBuffer);
		session.setNodeId(pkg.getNodeId());
		if (!pkg.isAnswer()) {// 连接到对端后收到对端发送的注册信息，此时应答自己的注册状态
			pkg = new NodeRegInfoPacket(session.cluster().getMyNodeId(), session.cluster().getClusterState(),
					session.cluster().getLastClusterStateTime(), session.cluster().getMyLeaderId(),
					ProxyRuntime.INSTANCE.getStartTime());
			pkg.setAnswer(true);
			session.answerClientNow(pkg);
		}
		session.cluster().onClusterNodeUp(pkg,session);
	}

}

package io.mycat.proxy.man.cmds;

import java.io.IOException;

import io.mycat.mycat2.ProxyStarter;
import io.mycat.proxy.ConfigKey;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster.ClusterState;
import io.mycat.proxy.man.packet.JoinCLusterAckPacket;
import io.mycat.proxy.man.packet.JoinCLusterNotifyPacket;

/**
 * 用来处理集群Jion报文的命令，
 * 
 * @author wuzhihui
 *
 */
public class ClusterJoinPacketCommand implements AdminCommand {
	protected static Logger logger = LoggerFactory.getLogger(ClusterJoinPacketCommand.class);

	@Override
	public void handlerPkg(AdminSession session, byte cmdType) throws IOException {
		if (cmdType == ManagePacket.PKG_JOIN_REQ_ClUSTER) {
			String nodeId = session.getNodeId();
			// nodeId为空，说明主节点为服务端，接受从节点注册加入集群，需要从节点发送节点信息
			if (nodeId == null) {
				NodeRegInfoPacket pkg = new NodeRegInfoPacket(session.cluster().getMyNodeId(), session.cluster().getClusterState(),
						session.cluster().getLastClusterStateTime(), session.cluster().getMyLeaderId(),
						ProxyRuntime.INSTANCE.getStartTime());
				pkg.setAnswer(false);
				session.answerClientNow(pkg);
				return;
			}
			ClusterNode theNode = session.cluster().findNode(nodeId);
			byte jionState = JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK;
			if (theNode.getMyClusterState() == ClusterState.Clustered) {
				jionState = JoinCLusterNotifyPacket.JOIN_STATE_ACKED;
			}
			JoinCLusterNotifyPacket respPacket = new JoinCLusterNotifyPacket(session.cluster().getMyAliveNodes(),
					ProxyRuntime.INSTANCE.getProxyConfig().getConfigVersion(ConfigKey.MYCAT_CONF));
			respPacket.setJoinState(jionState);
			session.answerClientNow(respPacket);

		} else if (cmdType == ManagePacket.PKG_JOIN_ACK_ClUSTER) {
			String nodeId = session.getNodeId();
			ClusterNode theNode = session.cluster().findNode(nodeId);
			if (theNode.getMyClusterState() != ClusterState.Clustered) {
				theNode.setMyClusterState(ClusterState.Clustered, System.currentTimeMillis());
			}

		} else if (cmdType == ManagePacket.PKG_JOIN_NOTIFY_ClUSTER) {
			// leader 批准加入Cluster
			int configVersion = ProxyRuntime.INSTANCE.getProxyConfig().getConfigVersion(ConfigKey.MYCAT_CONF);
			JoinCLusterNotifyPacket respPacket = new JoinCLusterNotifyPacket(null, configVersion);
			respPacket.resolve(session.readingBuffer);
			if (respPacket.getJoinState() == JoinCLusterNotifyPacket.JOIN_STATE_DENNIED) {
				logger.warn("Leader denied my join cluster request ");
			} else if (respPacket.getJoinState() == JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK) {
				ClusterNode node = session.cluster().findNode(session.getNodeId());
				session.cluster().setMyLeader(node);
				session.cluster().setClusterState(ClusterState.Clustered);
				JoinCLusterAckPacket ackPacket = new JoinCLusterAckPacket(session.cluster().getMyAliveNodes());
				session.answerClientNow(ackPacket);

				// 已加入集群，加载配置
				ProxyStarter.INSTANCE.startProxy(false);
			}

		} else {
			logger.warn("Maybe Buf,Leader us wan't you Fix it ");
		}
	}

}

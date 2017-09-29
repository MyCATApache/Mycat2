package io.mycat.proxy.man.cmds;

import java.io.IOException;

import io.mycat.proxy.man.packet.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.AdminCommand;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.MyCluster.ClusterState;

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
			logger.debug(" handler PKG_JOIN_REQ_ClUSTER package. from {} ",session.getNodeId());
			String nodeId = session.getNodeId();
			// nodeId为空，说明主节点为服务端，接受从节点注册加入集群，需要从节点发送节点信息
			if (nodeId == null) {
				sendRegInfo(session);
				return;
			}
			ClusterNode theNode = session.cluster().findNode(nodeId);
			byte joinState = JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK;
			if (theNode.getMyClusterState() == ClusterState.Clustered) {
				logger.debug(" send JOIN_STATE_ACKED package. to {} ",session.getNodeId());
				joinState = JoinCLusterNotifyPacket.JOIN_STATE_ACKED;
			}else{
				logger.debug(" send JOIN_STATE_NEED_ACK package. to {} ",session.getNodeId());
			}
			JoinCLusterNotifyPacket respPacket = new JoinCLusterNotifyPacket(session.cluster().getMyAliveNodes());
			respPacket.setJoinState(joinState);
			session.answerClientNow(respPacket);

		} else if (cmdType == ManagePacket.PKG_JOIN_ACK_ClUSTER) {
			logger.debug(" handler PKG_JOIN_ACK_ClUSTER package. from {} ",session.getNodeId());
			String nodeId = session.getNodeId();
			ClusterNode theNode = session.cluster().findNode(nodeId);
			if (theNode.getMyClusterState() != ClusterState.Clustered) {
				theNode.setMyClusterState(ClusterState.Clustered, System.currentTimeMillis());
			}
		} else if (cmdType == ManagePacket.PKG_JOIN_NOTIFY_ClUSTER) {
			logger.debug(" handler PKG_JOIN_NOTIFY_ClUSTER package. from {} ",session.getNodeId());
			// leader 批准加入Cluster
			JoinCLusterNotifyPacket respPacket = new JoinCLusterNotifyPacket(null);
			respPacket.resolve(session.readingBuffer);
			if (respPacket.getJoinState() == JoinCLusterNotifyPacket.JOIN_STATE_DENNIED) {
				logger.warn("Leader denied my join cluster request from {} ",session.getNodeId());
			} else if (respPacket.getJoinState() == JoinCLusterNotifyPacket.JOIN_STATE_NEED_ACK) {
				logger.debug(" handler JOIN_STATE_NEED_ACK package. from {} ",session.getNodeId());
				String nodeId = session.getNodeId();
				if (nodeId == null) {
					sendRegInfo(session);
					return;
				}

				if (session.cluster().getClusterState() == ClusterState.Clustered) {
					// 节点已经收到主节点的JOIN_STATE_NEED_ACK报文，直接返回
					logger.debug("node already in clustered state, this package may duplicate, ignore");
					return;
				}
				ClusterNode node = session.cluster().findNode(nodeId);
				session.cluster().setMyLeader(node);

				session.cluster().setClusterState(ClusterState.Clustered);
				logger.debug(" send join cluster ack package. to {} ",session.getNodeId());
				JoinCLusterAckPacket ackPacket = new JoinCLusterAckPacket(session.cluster().getMyAliveNodes());
				session.answerClientNow(ackPacket);

				logger.debug(" send config version req package. to ",session.getNodeId());
				// 已加入集群，加载配置
				ConfigVersionReqPacket versionReqPacket = new ConfigVersionReqPacket();
				session.answerClientNow(versionReqPacket);
			}

		} else {
			logger.warn("Maybe Buf,Leader us wan't you Fix it ");
		}
	}

	private void sendRegInfo(AdminSession session) throws IOException {
		logger.debug(" send nodeRegInfo package. to {} ",session.getNodeId());
		NodeRegInfoPacket pkg = new NodeRegInfoPacket(session.cluster().getMyNodeId(), session.cluster().getClusterState(),
				session.cluster().getLastClusterStateTime(), session.cluster().getMyLeaderId(),
				ProxyRuntime.INSTANCE.getStartTime());
		pkg.setAnswer(false);
		session.answerClientNow(pkg);
	}
}

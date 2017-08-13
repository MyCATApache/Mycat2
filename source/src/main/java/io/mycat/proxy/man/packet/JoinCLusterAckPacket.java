package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * Leader同意加入集群后，节点发送确认报文，完成加入过程
 * 
 * @author wuzhihui
 *
 */
public class JoinCLusterAckPacket extends ManagePacket {

	// 与我直连的所有Node的ID，逗号分隔
	private String myConnectedNodes;

	public JoinCLusterAckPacket(String myConnectedNodes) {
		super(ManagePacket.PKG_JOIN_ACK_ClUSTER);
		this.myConnectedNodes = myConnectedNodes;
	}

	public JoinCLusterAckPacket() {
		this(null);
	}

	public String[] getMyJoinedNodeIds() {
		return myConnectedNodes.split(",");
	}

	@Override
	public void resolveBody(ProxyBuffer buffer) {
		this.myConnectedNodes = buffer.readNULString();

	}

	@Override
	public void writeBody(ProxyBuffer buffer) {
		buffer.writeNULString(myConnectedNodes);

	}

}

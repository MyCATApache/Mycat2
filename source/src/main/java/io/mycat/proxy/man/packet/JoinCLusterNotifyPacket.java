package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * 应答Node加入集群的申请
 * 
 * @author wuzhihui
 *
 */
public class JoinCLusterNotifyPacket extends ManagePacket {
	public static final byte JOIN_STATE_DENNIED = -1;
	public static final byte JOIN_STATE_NEED_ACK = 0;
	public static final byte JOIN_STATE_ACKED = 1;
	// 与我直连的所有Node的ID，逗号分隔
	private String myConnectedNodes;

	private byte joinState = JOIN_STATE_NEED_ACK;

	public JoinCLusterNotifyPacket(String myConnectedNodes) {
		super(ManagePacket.PKG_JOIN_NOTIFY_ClUSTER);
		this.myConnectedNodes = myConnectedNodes;
	}

	public String[] getMyJoinedNodeIds() {
		return myConnectedNodes.split(",");
	}

	@Override
	public void resolveBody(ProxyBuffer buffer) {
		this.myConnectedNodes = buffer.readNULString();
		this.joinState = buffer.readByte();

	}

	@Override
	public void writeBody(ProxyBuffer buffer) {
		buffer.writeNULString(myConnectedNodes);
		buffer.writeByte(joinState);
	}

	public String getMyConnectedNodes() {
		return myConnectedNodes;
	}

	public void setMyConnectedNodes(String myConnectedNodes) {
		this.myConnectedNodes = myConnectedNodes;
	}

	public byte getJoinState() {
		return joinState;
	}

	public void setJoinState(byte joinState) {
		this.joinState = joinState;
	}

}

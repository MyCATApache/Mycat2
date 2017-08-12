package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * 节点信息的报文，用于向对方表明自己的身份信息
 * 
 * @author wuzhihui
 *
 */
public class NodeRegInfoPacket extends ManagePacket {
	private String nodeId;
	private long startupTime;
	//是否应答之前的NodeRegInfo报文
	private boolean isAnswer;

	public NodeRegInfoPacket(String nodeId, long startupTime) {
		super(ManagePacket.PKG_NODE_REG);
		this.nodeId = nodeId;
		this.startupTime = startupTime;
	}

	public NodeRegInfoPacket() {
		this(null, 0);
	}

	@Override
	public void resolveBody(ProxyBuffer buffer) {
		nodeId = buffer.readNULString();
		startupTime = buffer.readFixInt(8);
		isAnswer=buffer.readByte()==0x01;

	}

	@Override
	public void writeBody(ProxyBuffer buffer) {
		buffer.writeNULString(nodeId);
		buffer.writeFixInt(8, startupTime);
		buffer.writeByte((byte) (isAnswer?0x01:0x00));

	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public void setStartupTime(long startupTime) {
		this.startupTime = startupTime;
	}

	public boolean isAnswer() {
		return isAnswer;
	}

	public void setAnswer(boolean isAnswer) {
		this.isAnswer = isAnswer;
	}

}

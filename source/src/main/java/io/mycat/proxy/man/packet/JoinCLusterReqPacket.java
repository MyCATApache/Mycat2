package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * 向Leader申请加入集群的报文
 * 
 * @author wuzhihui
 *
 */
public class JoinCLusterReqPacket extends ManagePacket {

	// 与我直连的所有Node的ID，逗号分隔
	private String myConnectedNodes;

	public JoinCLusterReqPacket(String myConnectedNodes) {
		super(ManagePacket.PKG_JOIN_REQ_ClUSTER);
		this.myConnectedNodes = myConnectedNodes;
	}

	public JoinCLusterReqPacket() {
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

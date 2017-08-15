package io.mycat.proxy.man.packet;

import io.mycat.proxy.man.ManagePacket;
import io.mycat.proxy.man.ProtocolBuffer;

/**
 * 执行成功的Packet报文
 * 
 * @author wuzhihui
 *
 */
public class SuccessPacket extends ManagePacket {

	public SuccessPacket(String tips) {
		this();
		this.tips = tips;
	}

	public SuccessPacket() {
		super(ManagePacket.PKG_SUCCESS);
	}

	// 报文成功后的提示内容
	private String tips;

	@Override
	public void resolveBody(ProtocolBuffer buffer) {
		this.tips = buffer.readNULString();

	}

	@Override
	public void writeBody(ProtocolBuffer buffer) {
		buffer.writeNULString(tips);

	}

	public String getTips() {
		return tips;
	}

	public void setTips(String tips) {
		this.tips = tips;
	}

}

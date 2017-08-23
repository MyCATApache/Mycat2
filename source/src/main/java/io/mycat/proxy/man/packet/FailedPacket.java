package io.mycat.proxy.man.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.man.ManagePacket;

/**
 * 执行失败的Packet报文
 * 
 * @author wuzhihui
 *
 */
public class FailedPacket extends ManagePacket {

	public FailedPacket(int errorCode, String errMsg) {
		this();
		this.errorCode = errorCode;
		this.errMsg = errMsg;
	}

	public FailedPacket() {
		super(ManagePacket.PKG_FAILED);

	}

	private int errorCode;
	private String errMsg;

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}
	
	@Override
	public void resolveBody(ProxyBuffer buffer) {
		this.errorCode = (int) buffer.readFixInt(4);
		this.errMsg = buffer.readNULString();

	}

	@Override
	public void writeBody(ProxyBuffer buffer) {
		buffer.writeFixInt(4, errorCode);
		buffer.writeNULString(errMsg);

	}

}

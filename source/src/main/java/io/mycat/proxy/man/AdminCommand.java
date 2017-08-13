package io.mycat.proxy.man;

import java.io.IOException;

/**
 * 负责解析请求的命令报文并且正确应答报文
 * @author wuzhihui
 *
 */
public interface AdminCommand {

	void handlerPkg(AdminSession session,byte cmdType) throws IOException;
}

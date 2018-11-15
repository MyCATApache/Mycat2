package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 此NIO Handler应该是单例的，能为多个Session会话服务,Handler的状态数据则保存在Session中，状态数据分为当前请求的SQL相关的，
 * 以及Session生命周期相关的，后者作为Session的属性，需要谨慎Careful添加，核心commiter务必认真review志愿者对Session属性的扩展
 * 避免内存消耗问题。
 * 
 * @author wuzhihui
 *
 */
public interface NIOHandler<T extends Session> {

	void onConnect(SelectionKey curKey, T session, boolean success, String msg) throws IOException;

	void onSocketRead(T session) throws IOException;

	void onSocketWrite(T session) throws IOException;

	public void onWriteFinished(T s) throws IOException;

	void onSocketClosed(T session,boolean normal);
}

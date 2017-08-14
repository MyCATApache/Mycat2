package io.mycat.proxy;
/**
 * 标记类的接口，下面有两个子接口
 * 注意：此NIO Handler应该是单例的，能为多个Session会话服务
 * @author wuzhihui
 *
 */
public interface NIOHandler<T extends Session> {

}

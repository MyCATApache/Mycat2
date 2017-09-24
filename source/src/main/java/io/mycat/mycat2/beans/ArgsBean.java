package io.mycat.mycat2.beans;

/**
 * Desc: 命令行参数
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class ArgsBean {
    public static final String PROXY_PORT = "-mycat.proxy.port";

    public static final String CLUSTER_ENABLE = "-mycat.cluster.enable";
    public static final String CLUSTER_PORT = "-mycat.cluster.port";
    public static final String CLUSTER_MY_NODE_ID = "-mycat.cluster.myNodeId";

    public static final String BALANCER_ENABLE = "-mycat.balancer.enable";
    public static final String BALANCER_PORT = "-mycat.balancer.port";
    public static final String BALANCER_STRATEGY = "-mycat.proxy.strategy";
}

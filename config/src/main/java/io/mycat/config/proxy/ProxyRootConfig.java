package io.mycat.config.proxy;


import io.mycat.config.Configurable;

/**
 * Desc: 对应mycat.yml文件
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class ProxyRootConfig implements Configurable {
    private ProxyConfig proxy;

    public ProxyConfig getProxy() {
        return proxy;
    }

    public void setProxy(ProxyConfig proxy) {
        this.proxy = proxy;
    }
}

package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

/**
 * Desc: 对应mycat.yml文件
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class ProxyConfig implements Configurable {
    private ProxyBean proxy;

    public ProxyBean getProxy() {
        return proxy;
    }

    public void setProxy(ProxyBean proxy) {
        this.proxy = proxy;
    }
}

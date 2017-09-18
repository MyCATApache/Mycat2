package io.mycat.proxy.man;

/**
 * Desc: 用于记录集群中prepare commit提交时的状态
 *
 * @date: 18/09/2017
 * @author: gaozhiwen
 */
public class ConfigConfirmBean {
    public int confirmCount;
    public int confirmVersion;

    public ConfigConfirmBean(int confirmCount, int confirmVersion) {
        this.confirmCount = confirmCount;
        this.confirmVersion = confirmVersion;
    }
}

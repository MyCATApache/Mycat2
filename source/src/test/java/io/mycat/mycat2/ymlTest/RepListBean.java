package io.mycat.mycat2.ymlTest;

import java.util.List;

/**
 * Desc:
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class RepListBean {
    private List<RepBean> mysqlReplicas;

    public List<RepBean> getMysqlReplicas() {
        return mysqlReplicas;
    }

    public void setMysqlReplicas(List<RepBean> mysqlReplicas) {
        this.mysqlReplicas = mysqlReplicas;
    }

    @Override public String toString() {
        return "RepListBean{" +
                "mysqlReplicas=" + mysqlReplicas +
                '}';
    }
}
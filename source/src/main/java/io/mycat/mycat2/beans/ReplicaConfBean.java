package io.mycat.mycat2.beans;

import java.util.List;

/**
 * Desc: 用于加载datasource.yml的类
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class ReplicaConfBean {
    private List<MySQLRepBean> mysqlReplicas;

    public List<MySQLRepBean> getMysqlReplicas() {
        return mysqlReplicas;
    }

    public void setMysqlReplicas(List<MySQLRepBean> mysqlReplicas) {
        this.mysqlReplicas = mysqlReplicas;
    }
}

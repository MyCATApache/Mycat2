/*
 * Copyright (c) 2016, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://mycat.io/
 *
 */
package io.mycat.mycat2.beans;

import io.mycat.mycat2.MycatConfig;
import io.mycat.proxy.ProxyRuntime;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 表示一組MySQL Server复制集群，如主从或者多主
 *
 * @author wuzhihui
 */
public class MySQLRepBean {
    public enum RepTypeEnum {
        MASTER_SLAVE, MASTER_MASTER;
        RepTypeEnum() {}
    }

    public enum RepSwitchTypeEnum {
        SLAVE_ONLY, MASTER_ONLY;
        RepSwitchTypeEnum() {}
    }

    private String name;
    private RepTypeEnum type;
    private RepSwitchTypeEnum switchType;
    private List<MySQLMetaBean> mysqls;

    private int writeIndex = 0; //主节点默认为0

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RepTypeEnum getType() {
        return type;
    }

    public void setType(RepTypeEnum type) {
        this.type = type;
    }

    public RepSwitchTypeEnum getSwitchType() {
        return switchType;
    }

    public void setSwitchType(RepSwitchTypeEnum switchType) {
        this.switchType = switchType;
    }

    public List<MySQLMetaBean> getMysqls() {
        return mysqls;
    }

    public void setMysqls(List<MySQLMetaBean> mysqls) {
        this.mysqls = mysqls;
    }

    public void initMaster() {
        // 根据配置replica-index的配置文件修改主节点
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        Integer repIndex = conf.getRepIndex(name);
        if (repIndex != null) {
            writeIndex = repIndex;
        }
        mysqls.get(writeIndex).setSlaveNode(false);
    }

    /**
     * 得到当前用于写的MySQLMetaBean
     */
    public MySQLMetaBean getCurWriteMetaBean() {
        return mysqls.get(writeIndex);
    }

    /**
     * 得到当前用于读的MySQLMetaBean（负载均衡模式，如果支持）
     */
    public MySQLMetaBean getLBReadMetaBean() {
        return mysqls.get(ThreadLocalRandom.current().nextInt(mysqls.size()));
    }

    @Override
    public String toString() {
        return "MySQLRepBean [name=" + name + ", type=" + type + ", switchType=" + switchType + ", mysqls=" + mysqls + "]";
    }
}

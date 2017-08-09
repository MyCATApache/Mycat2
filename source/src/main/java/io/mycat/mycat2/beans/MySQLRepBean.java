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

import java.util.Collections;
import java.util.List;

/**
 * 表示一組MySQL Server复制集群，如主从或者多主
 *
 * @author wuzhihui
 */
public class MySQLRepBean {
    private final String name;
    private final int type;
    private int switchType;
    private List<MySQLBean> mysqls = Collections.emptyList();

    public MySQLRepBean(String name, int type) {
        super();
        this.name = name;
        this.type = type;
    }

    public int getSwitchType() {
        return switchType;
    }

    public void setSwitchType(int switchType) {
        this.switchType = switchType;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public List<MySQLBean> getMysqls() {
        return mysqls;
    }

    public void setMysqls(List<MySQLBean> mysqls) {
        this.mysqls = mysqls;
    }

    @Override
    public String toString() {
        return "MySQLRepBean [name=" + name + ", type=" + type + ", switchType=" + switchType + ", mysqls=" + mysqls + "]";
    }


}

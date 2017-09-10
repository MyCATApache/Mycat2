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
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.mycat2.beans;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.tasks.BackendCharsetReadTask;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 后端mysql连接元数据类，对应datasource.xml配置中的mysql元数据信息
 * @author wuzhihui
 */
public class MySQLMetaBean {
    private String hostName;
    private String ip;
    private int port;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;
    private boolean slaveNode = true; // 默认为slave节点

    public boolean charsetLoaded = false;

    /** collationIndex 和 charsetName 的映射 */
    public final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();
    /** charsetName 到 默认collationIndex 的映射 */
    public final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

    public MySQLMetaBean(String ip, int port, String user, String password) {
        super();
        this.hostName = ip;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public MySQLMetaBean() {}

    public void init() throws IOException {
        ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        ProxyReactorThread[] reactorThreads = runtime.getReactorThreads();
        int reactorSize = runtime.getNioReactorThreads();
        for (int i = 0; i < minCon; i++) {
            ProxyReactorThread reactorThread = reactorThreads[i % reactorSize];
            reactorThread.createSession(this, null, (optSession, sender, exeSucces, retVal) -> {
                MySQLSession session = (MySQLSession) optSession;
                if (exeSucces) {
                    //设置当前连接 读写分离属性
                    session.setDefaultChannelRead(this.isSlaveNode());
                    if (this.charsetLoaded == false) {
                        this.charsetLoaded = true;
                        BackendCharsetReadTask backendCharsetReadTask = new BackendCharsetReadTask(session, this);
                        optSession.setCurNIOHandler(backendCharsetReadTask);
                        backendCharsetReadTask.readCharset();
                    }
                    optSession.change2ReadOpts();
                    reactorThread.addMySQLSession(this, session);
                }
            });
        }
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    public int getMinCon() {
        return minCon;
    }

    public void setMinCon(int minCon) {
        this.minCon = minCon;
    }

    public boolean isSlaveNode() {
        return slaveNode;
    }

    public void setSlaveNode(boolean slaveNode) {
        slaveNode = slaveNode;
    }

    @Override
    public String toString() {
        return "MySQLMetaBean [hostName=" + hostName + ", ip=" + ip + ", port=" + port + ", user=" + user + ", password="
                + password + ", maxCon=" + maxCon + ", minCon=" + minCon + ", slaveNode=" + slaveNode + "]";
    }

}

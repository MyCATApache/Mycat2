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

/**
 * @author wuzhihui
 */
public class MySQLBean {
    private String hostName;
    private String ip;
    private int port;
    private String user;
    private String password;
    private int maxCon = 1000;
    private int minCon = 1;

    public MySQLBean(String ip, int port, String user, String password) {
        super();
        this.hostName = ip;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public MySQLBean() {

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

    @Override
    public String toString() {
        return "MySQLBean [hostName=" + hostName + ", ip=" + ip + ", port=" + port + ", user=" + user + ", password="
                + password + ", maxCon=" + maxCon + ", minCon=" + minCon + "]";
    }

}

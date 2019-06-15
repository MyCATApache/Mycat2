/**
 * Copyright (C) <2019>  <gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.config.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Desc: mycat代理配置类
 *
 * date: 24/09/2017
 * @author: gaozhiwen
 */
public class ProxyConfig {

    private static final short DEFAULT_BUFFER_CHUNK_SIZE = 1024 * 4 * 2;
    private static final int DEFAULT_BUFFER_POOL_PAGE_SIZE = 1024 * 1024 * 4;
    private static final short DEFAULT_BUFFER_POOL_PAGE_NUMBER = 64;
    private static final Logger logger = LoggerFactory.getLogger(ProxyConfig.class);

    /**
     * 绑定的数据传输IP地址
     */
    private String ip = "0.0.0.0";
    /**
     * 绑定的数据传输端口
     */
    private int port = 8066;

    // a page size
    private int bufferPoolPageSize = DEFAULT_BUFFER_POOL_PAGE_SIZE;

    //minimum allocation unit
    private short bufferPoolChunkSize = DEFAULT_BUFFER_CHUNK_SIZE;

    // buffer pool page number 
    private short bufferPoolPageNumber = DEFAULT_BUFFER_POOL_PAGE_NUMBER;

    private int reactorNumber = -1;

    public int getReactorNumber() {
        return reactorNumber == -1 ? Runtime.getRuntime().availableProcessors() : reactorNumber;
    }

    public void setReactorNumber(int reactorNumber) {
        this.reactorNumber = reactorNumber;
    }

    /**
     * 是否使用动态配置的开关
     */
    private boolean annotationEnable;

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

    public boolean isAnnotationEnable() {
        return annotationEnable;
    }

    public void setAnnotationEnable(boolean annotationEnable) {
        this.annotationEnable = annotationEnable;
    }

    @Override
    public String toString() {
        return "ProxyConfig{" + "ip='" + ip + '\'' + ", port=" + port + ",annotationEnable=" + annotationEnable + "}";
    }

    public int getBufferPoolPageSize() {
        return bufferPoolPageSize;
    }

    public void setBufferPoolPageSize(int bufferPoolPageSize) {
        this.bufferPoolPageSize = bufferPoolPageSize;
    }

    public short getBufferPoolChunkSize() {
        return bufferPoolChunkSize;
    }

    public void setBufferPoolChunkSize(short bufferPoolChunkSize) {
        if (bufferPoolChunkSize < 86) {
            ///cjw  2018.4.6 fix the HandshakePacket write proxybuffer which is low than 86 lead to error
            logger.warn("bufferPoolChunkSize should be greater than 86,and will be updated to 128;");
            bufferPoolChunkSize = 128;
        }
        this.bufferPoolChunkSize = bufferPoolChunkSize;
    }

    public short getBufferPoolPageNumber() {
        return bufferPoolPageNumber;
    }

    public void setBufferPoolPageNumber(short bufferPoolPageNumber) {
        this.bufferPoolPageNumber = bufferPoolPageNumber;
    }

}

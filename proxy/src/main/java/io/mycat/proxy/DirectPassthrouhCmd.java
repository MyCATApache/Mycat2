/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.proxy;

import io.mycat.beans.DataNode;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.command.MySQLProxyCommand;
import io.mycat.proxy.packet.ErrorCode;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.ByteArrayView;
import io.mycat.router.RouteResult;

import java.io.IOException;

public class DirectPassthrouhCmd implements MySQLProxyCommand {
    public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();

    @Override
    public boolean procssSQL(MycatSession mycat) throws IOException {
        ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
        RouteResult route = mycat.route((ByteArrayView) proxyBuffer);
        DataNode dataNode = mycat.getSchema().getDefaultDataNode();
        if(!route.isSQLChanged()){
           proxyBuffer.channelWriteStartIndex(0);
           proxyBuffer.channelWriteEndIndex(proxyBuffer.channelReadEndIndex());
       }else {
           throw new MycatExpection(ErrorCode.ER_UNKNOWN_ERROR,"unsupport!!");
       }
       if (route.getDataNodeName() != null){
           dataNode = MycatRuntime.INSTANCE.getMycatConfig().getDataNodeByName(route.getDataNodeName());
       }
        mycat.setRequestFinished(true);
        mycat.getSingleBackendAndCallBack(false, dataNode ,null, (mysql, sender, success, result, throwable) -> {
            if (success) {
                mycat.clearReadWriteOpts();
                try {
                    mysql.writeToChannel(proxyBuffer);
                }catch (IOException e){
                    mycat.closeAllBackendsAndResponseError("");
                }
            } else {
                mycat.closeAllBackendsAndResponseError("");
            }
        });
        return false;
    }

    @Override
    public boolean onBackendResponse(MySQLSession mysql) throws IOException {
        if (!mysql.readFromChannel()) {
            return false;
        }
        ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
        MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
        MySQLPacketResolver packetResolver = mysql.getPacketResolver();

        int startIndex = mySQLPacket.packetReadStartIndex();
        int endPos = startIndex;
        while (mysql.readMySQLPacket()) {
            endPos = packetResolver.getEndPos();
            mySQLPacket.packetReadStartIndex(endPos);
        }
        proxyBuffer.channelWriteStartIndex(startIndex);
        proxyBuffer.channelWriteEndIndex(endPos);
        mysql.getMycatSession().writeToChannel();
        return false;
    }

    @Override
    public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
        return false;
    }

    @Override
    public boolean onFrontWriteFinished(MycatSession mycat) throws IOException {
        MySQLSession mysql = mycat.getBackend();
        if (mysql.isResponseFinished()) {
            mycat.change2ReadOpts();
            mysql.clearReadWriteOpts();
            return true;
        } else {
            mysql.change2ReadOpts();
            mycat.clearReadWriteOpts();
            ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
            int writeEndIndex = proxyBuffer.channelWriteEndIndex();
            proxyBuffer.channelReadStartIndex(writeEndIndex);
        }
        return false;
    }

    @Override
    public boolean onBackendWriteFinished(MySQLSession mysql) throws IOException {
        MycatSession mycat = mysql.getMycatSession();
        if (mycat.isRequestFinished()) {
            //mysql.clearReadWriteOpts();
            mysql.currentProxyBuffer().reset();
            mysql.currentProxyBuffer().newBuffer();
            mysql.prepareReveiceResponse();
            mysql.change2ReadOpts();
        } else {
            // mycat.clearReadWriteOpts();
            mysql.change2WriteOpts();
            mysql.writeToChannel();
        }
        return false;
    }

    @Override
    public void clearResouces(MycatSession mycat, boolean sessionCLosed) {
        MySQLSession backend = mycat.getBackend();
        backend.unbindMycatIfNeed(mycat);
    }

    @Override
    public void clearResouces(MySQLSession mysql, boolean sessionCLosed) {

    }

}

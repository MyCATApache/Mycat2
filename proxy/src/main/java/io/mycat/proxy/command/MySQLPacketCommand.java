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
package io.mycat.proxy.command;

import io.mycat.beans.DataNode;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-04-30 22:33
 **/
public class MySQLPacketCommand implements MySQLProxyCommand{
  private static final DirectPassthrouhCmd directPassthrouhCmd = new DirectPassthrouhCmd();
  public static final MySQLPacketCommand INSTANCE = new MySQLPacketCommand();

  @Override
  public boolean handle(MycatSession mycat) throws IOException {
    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
    proxyBuffer.channelWriteStartIndex(0);
    proxyBuffer.channelWriteEndIndex(proxyBuffer.channelReadEndIndex());
    DataNode dataNode = mycat.getSchema().getDefaultDataNode();
    directPassthrouhCmd.writeProxyBufferToDataNode(mycat,proxyBuffer,dataNode);
    return false;
  }
  @Override
  public boolean onBackendResponse(MySQLSession session) throws IOException {
    return directPassthrouhCmd.onBackendResponse(session);
  }

  @Override
  public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
    return directPassthrouhCmd.onBackendClosed(session,normal);
  }

  @Override
  public boolean onFrontWriteFinished(MycatSession session) throws IOException {
    return directPassthrouhCmd.onFrontWriteFinished(session);
  }

  @Override
  public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
    return directPassthrouhCmd.onBackendWriteFinished(session);
  }

  @Override
  public void clearResouces(MycatSession session, boolean sessionCLosed) {
     directPassthrouhCmd.clearResouces(session,sessionCLosed);
  }

  @Override
  public void clearResouces(MySQLSession session, boolean sessionCLosed) {
    directPassthrouhCmd.clearResouces(session,sessionCLosed);
  }

}

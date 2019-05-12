/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.task;

import io.mycat.MycatExpection;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import java.io.IOException;

public interface ResultSetTask extends NIOHandler<MySQLClientSession>, MySQLPacketCallback {

  default void request(MySQLClientSession mysql, int head, byte[] data,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    assert (mysql.currentProxyBuffer() == null);
    if (data.length > mysql.getMycatReactorThread().getBufPool().getChunkSize()) {
      throw new MycatExpection("ResultSetTask unsupport request length more than 1024 bytes");
    }
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getMycatReactorThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(data.length + 5);
    mySQLPacket.writeByte((byte) head);
    mySQLPacket.writeBytes(data);
    request(mysql, callBack, mySQLPacket, mysql.setPacketId(0));
  }

  default void request(MySQLClientSession mysql, AsynTaskCallBack<MySQLClientSession> callBack,
      MySQLPacket mySQLPacket, int b) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      mysql.prepareReveiceResponse();
      mysql.writeProxyPacket(mySQLPacket, b);
    } catch (IOException e) {
      this.clearAndFinished(mysql, false, e.getMessage());
    }
  }

  default void request(MySQLClientSession mysql, int head, long data,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    assert (mysql.currentProxyBuffer() == null);
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getMycatReactorThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(12);
    mySQLPacket.writeByte((byte) head);
    mySQLPacket.writeFixInt(4, data);

    request(mysql, callBack, mySQLPacket, 0);
  }

  default void requestEmptyPacket(MySQLClientSession mysql, byte nextPacketId,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    assert (mysql.currentProxyBuffer() == null);
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getMycatReactorThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(4);
    request(mysql, callBack, mySQLPacket, nextPacketId);
  }

  default void request(MySQLClientSession mysql, byte[] packetData,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      assert (mysql.currentProxyBuffer() == null);
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getMycatReactorThread().getBufPool()));
      mysql.prepareReveiceResponse();
      mysql.writeProxyBufferToChannel(packetData);
    } catch (IOException e) {
      this.clearAndFinished(mysql, false, e.getMessage());
    }
  }

  default void request(MySQLClientSession mysql, int head, String data,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    request(mysql, head, data.getBytes(), callBack);
  }


  default void onFinished(MySQLClientSession mysql, boolean success, String errorMessage) {
    AsynTaskCallBack callBack = mysql.getCallBackAndReset();
    callBack.finished(mysql, this, success, getResult(), errorMessage);
  }

  default Object getResult() {
    return null;
  }

  @Override
  default void onSocketRead(MySQLClientSession mysql) throws IOException {
    try {
      if (mysql.getCurNIOHandler() != this) {
        return;
      }
      MySQLPacketResolver resolver = mysql.getPacketResolver();
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
      proxyBuffer.newBufferIfNeed();
      if (!mysql.readFromChannel()) {
        return;
      }
      int totalPacketEndIndex = proxyBuffer.channelReadEndIndex();
      MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
      boolean isResponseFinished = false;
      while (mysql.getCurNIOHandler() == this && mysql.readProxyPayloadFully()) {
        MySQLPayloadType type = mysql.getPacketResolver().getMySQLPayloadType();
        isResponseFinished = mysql.isResponseFinished();
        MySQLPacket payload = mysql.currentProxyPayload();
        int startPos = payload.packetReadStartIndex();
        int endPos = payload.packetReadEndIndex();
        switch (type) {
          case REQUEST:
            this.onRequest(mySQLPacket, startPos, endPos);
            break;
          case LOAD_DATA_REQUEST:
            this.onLoadDataRequest(mySQLPacket, startPos, endPos);
            break;
          case REQUEST_SEND_LONG_DATA:
            this.onPrepareLongData(mySQLPacket, startPos, endPos);
            break;
          case REQUEST_COM_STMT_CLOSE:
            break;
          case FIRST_ERROR:
            this.onError(mySQLPacket, startPos, endPos);
            break;
          case FIRST_OK:
            this.onOk(mySQLPacket, startPos, endPos);
            break;
          case FIRST_EOF:
            this.onEof(mySQLPacket, startPos, endPos);
            break;
          case COLUMN_COUNT:
            this.onColumnCount(resolver.getColumnCount());
            break;
          case COLUMN_DEF:
            this.onColumnDef(mySQLPacket, startPos, endPos);
            break;
          case COLUMN_EOF:
            this.onColumnDefEof(mySQLPacket, startPos, endPos);
            break;
          case TEXT_ROW:
            this.onTextRow(mySQLPacket, startPos, endPos);
            break;
          case BINARY_ROW:
            this.onBinaryRow(mySQLPacket, startPos, endPos);
            break;
          case ROW_EOF:
            this.onRowEof(mySQLPacket, startPos, endPos);
            break;
          case ROW_FINISHED:
            break;
          case ROW_OK:
            this.onRowOk(mySQLPacket, startPos, endPos);
            break;
          case ROW_ERROR:
            this.onRowError(mySQLPacket, startPos, endPos);
            break;
          case PREPARE_OK:
            this.onPrepareOk(resolver);
            break;
          case PREPARE_OK_PARAMER_DEF:
            this.onPrepareOkParameterDef(mySQLPacket, startPos, endPos);
            break;
          case PREPARE_OK_COLUMN_DEF:
            this.onPrepareOkColumnDef(mySQLPacket, startPos, endPos);
            break;
          case PREPARE_OK_COLUMN_DEF_EOF:
            this.onPrepareOkColumnDefEof(resolver);
            break;
          case PREPARE_OK_PARAMER_DEF_EOF:
            this.onPrepareOkColumnDefEof(resolver);
            break;
        }
        mysql.resetCurrentProxyPayload();
        proxyBuffer.channelReadEndIndex(totalPacketEndIndex);
        if (isResponseFinished) {
          break;
        } else if (mysql.getCurNIOHandler() == this) {
          MySQLPacketResolver packetResolver = mysql.getPacketResolver();
          mySQLPacket.packetReadStartIndex(packetResolver.getEndPos());
        }
      }
      if (isResponseFinished) {
        clearAndFinished(mysql, true, null);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      clearAndFinished(mysql, false, e.getMessage());
    }
  }

  default void clearAndFinished(MySQLClientSession mysql, boolean success, String errorMessage) {
    mysql.resetPacket();
    mysql.setCurrentProxyBuffer(null);
    mysql.switchDefaultNioHandler();
    onFinished(mysql, success, errorMessage);
  }

  @Override
  default void onWriteFinished(MySQLClientSession mysql) throws IOException {
    mysql.resetPacket();
    mysql.change2ReadOpts();
  }

  @Override
  default void onSocketClosed(MySQLClientSession session, boolean normal, String reasion) {
    onFinished(session, false, reasion);
  }
}

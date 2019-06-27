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
package io.mycat.proxy.handler.backend;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.BackendNIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.session.MySQLClientSession;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务类接口 该类实现文本结果集的命令发送以及解析处理
 *
 * @author jamie12221 date 2019-05-13 12:48
 */
public interface ResultSetHandler extends BackendNIOHandler<MySQLClientSession>,
    MySQLPacketCallback {
  static final byte[] EMPTY = new byte[]{};
  Logger logger = LoggerFactory.getLogger(BackendConCreateHandler.class);
  ResultSetHandler DEFAULT = new ResultSetHandler() {

  };

  /**
   * COM_QUERY 命令请求报文调用此方法 head是payload的第一个字节 data根据实际报文构造 该方法自动构造请求报文,生成报文序列号以及长度,
   * 但是被限制整个报文长度不超过proxybuffer的chunk大小,大小也不应该超过mysql拆分报文大小 如需构造大的报文,可以自行替换proxbuffer即可
   */
  default void request(MySQLClientSession mysql, int head, byte[] data,
      ResultSetCallBack<MySQLClientSession> callBack) {
    if (data == null){
      data = EMPTY;
    }
    assert (mysql.currentProxyBuffer() == null);
    int chunkSize = mysql.getIOThread().getBufPool().getChunkSize();
    if (data.length > (chunkSize - 5) || data.length > MySQLPacketSplitter.MAX_PACKET_SIZE) {
      throw new MycatExpection("ResultSetHandler unsupport request length more than 1024 bytes");
    }
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getIOThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(data.length + 5);
    mySQLPacket.writeByte((byte) head);
    mySQLPacket.writeBytes(data);
    request(mysql, mySQLPacket, mysql.setPacketId(0), callBack);
  }

  /**
   * @param mySQLPacket 该packet满足以下格式头四个字节留空 mySQLPacket是proxybuffer,也不应该超过mysql拆分报文大小
   */
  default void request(MySQLClientSession mysql, MySQLPacket mySQLPacket, int packetId,
      ResultSetCallBack<MySQLClientSession> callBack) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      mysql.setRequestSuccess(false);
      mysql.prepareReveiceResponse();
      mysql.writeCurrentProxyPacket(mySQLPacket, packetId);
    } catch (Exception e) {
      MycatMonitor.onResultSetWriteException(mysql, e);
      onException(mysql, e);
      callBack.onFinishedException(e, this, null);
    }
  }

  /**
   * 满足payload byte + long格式的请求
   */
  default void request(MySQLClientSession mysql, int head, long data,
      ResultSetCallBack<MySQLClientSession> callBack) {
    assert (mysql.currentProxyBuffer() == null);
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getIOThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(12);
    mySQLPacket.writeByte((byte) head);
    mySQLPacket.writeFixInt(4, data);
    request(mysql, mySQLPacket, 0, callBack);
  }

  /**
   * loaddata empty packet
   *
   * @param nextPacketId content of file后的packetId
   */
  default void requestEmptyPacket(MySQLClientSession mysql, byte nextPacketId,
      ResultSetCallBack<MySQLClientSession> callBack) {
    assert (mysql.currentProxyBuffer() == null);
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getIOThread().getBufPool()));
    MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(4);
    request(mysql, mySQLPacket, nextPacketId, callBack);
  }

  /**
   * @param packetData 包含报文头部的完整报文(不是payload)
   */
  default void request(MySQLClientSession mysql, byte[] packetData,
      ResultSetCallBack<MySQLClientSession> callBack) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      assert (mysql.currentProxyBuffer() == null);
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(mysql.getIOThread().getBufPool()));
      mysql.prepareReveiceResponse();
      mysql.writeProxyBufferToChannel(packetData);
    } catch (Exception e) {
      MycatMonitor.onResultSetWriteException(mysql, e);
      ResultSetCallBack callBackAndReset = mysql.getCallBack();
      onFinishedCollectException(mysql, e);
      onException(mysql, e);
      callBackAndReset.onFinishedException(e, this, null);
    }
  }

  /**
   * 一般用于com query
   */
  default void request(MySQLClientSession mysql, int head, String data,
      ResultSetCallBack<MySQLClientSession> callBack) {
    try {
      request(mysql, head, data.getBytes(), callBack);
    } catch (Exception e) {
      MycatMonitor.onResultSetWriteException(mysql, e);
      onException(mysql, e);
      callBack.onFinishedException(e, this, null);
    }
  }

//  /**
//   * 该方法可能会被重写
//   */
//  default void onFinishedCollect(MySQLClientSession mysql, boolean success, String errorMessage) {
//    ResultSetCallBack callBack = mysql.getCallBackAndReset();
//    assert callBack != null;
//    if (success) {
//      callBack.onFinishedCollect(mysql, this, true, getResult(), null);
//    } else {
//      callBack.onFinishedCollect(mysql, this, false, errorMessage, null);
//    }
//  }

  /**
   * 读事件处理
   */
  @Override
  default void onSocketRead(MySQLClientSession mysql) {
    assert mysql.getCurNIOHandler() == this;
    if (!mysql.isOpen()) {
      ResultSetCallBack callBackAndReset = mysql.getCallBack();
      ClosedChannelException closedChannelException = new ClosedChannelException();
      onException(mysql, closedChannelException);
      callBackAndReset.onFinishedException(closedChannelException, this, null);
      return;
    }
    try {
      MySQLPacketResolver resolver = mysql.getPacketResolver();
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
      proxyBuffer.newBufferIfNeed();
      if (!mysql.readFromChannel()) {
        return;
      }
      mysql.setRequestSuccess(true);
      int totalPacketEndIndex = proxyBuffer.channelReadEndIndex();
      MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
      boolean isResponseFinished = false;
      ErrorPacketImpl errorPacket = null;
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
          case REQUEST_COM_QUERY:
            this.onRequestComQuery(mySQLPacket, startPos, endPos);
            break;
          case REQUEST_SEND_LONG_DATA:
            this.onPrepareLongData(mySQLPacket, startPos, endPos);
            break;
          case REQUEST_PREPARE:
            this.onReqeustPrepareStatement(mySQLPacket, startPos, endPos);
            break;
          case REQUEST_COM_STMT_CLOSE:
            this.onRequestComStmtClose(mySQLPacket, startPos, endPos);
            break;
          case FIRST_ERROR: {

            ErrorPacketImpl packet = new ErrorPacketImpl();
            errorPacket = packet;
            packet.readPayload(mySQLPacket);
            this.onFirstError(packet);
            break;
          }
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
            ErrorPacketImpl packet = new ErrorPacketImpl();
            errorPacket = packet;
            packet.readPayload(mySQLPacket);
            this.onRowError(packet, startPos, endPos);
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
            this.onPrepareOkParameterDefEof(resolver);
            break;
        }
        mysql.resetCurrentProxyPayload();
        proxyBuffer.channelReadEndIndex(totalPacketEndIndex);
        if (isResponseFinished) {
          break;
        }
        assert mysql.getCurNIOHandler() == this;
        MySQLPacketResolver packetResolver = mysql.getPacketResolver();
        mySQLPacket.packetReadStartIndex(packetResolver.getEndPos());
      }
      if (isResponseFinished) {
        ResultSetCallBack callBackAndReset = mysql.getCallBack();
        mysql.setCallBack(null);
        onFinishedCollect(mysql);
        onClear(mysql);
        if (errorPacket == null) {
          callBackAndReset.onFinished(mysql.isMonopolized(), mysql, this, null);
        } else {
          callBackAndReset.onErrorPacket(errorPacket, mysql.isMonopolized(), mysql, this, null);
        }
        return;
      }
    } catch (Exception e) {
      logger.error("",e);
      ResultSetCallBack callBackAndReset = mysql.getCallBack();
      Objects.requireNonNull(callBackAndReset);
      mysql.setCallBack(null);
      if (mysql.isRequestSuccess()) {
        MycatMonitor.onResultSetReadException(mysql, e);
        onFinishedCollectException(mysql, e);
        onException(mysql, e);
        callBackAndReset.onFinishedException(e, this, null);
        return;
      } else {
        MycatMonitor.onResultSetWriteException(mysql, e);
        onFinishedCollectException(mysql, e);
        onException(mysql, e);
        callBackAndReset.onFinishedSendException(e, this, null);
        return;
      }

    }
  }

  /**
   * 该方法可能被重写,导致资源不释放
   */
  default void onClear(MySQLClientSession mysql) {
    mysql.resetPacket();
    mysql.switchNioHandler(null);
    mysql.setCallBack(null);
    MycatMonitor.onResultSetClear(mysql);
  }

  default void close(MySQLClientSession mysql, Exception e) {
    mysql.close(false, e);
  }

  /**
   * 向mysql服务器写入结束,切换到读事件
   */
  @Override
  default void onWriteFinished(MySQLClientSession mysql) {
    ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
    //写入完毕不清理buffer但是要把下标重置
    proxyBuffer.channelReadStartIndex(0);
    proxyBuffer.channelReadEndIndex(0);
    mysql.change2ReadOpts();
  }

  @Override
  default void onSocketWrite(MySQLClientSession mysql) {
    try {
      mysql.writeToChannel();
    } catch (Exception e) {
      MycatMonitor.onResultSetWriteException(mysql, e);
      ResultSetCallBack callBackAndReset = mysql.getCallBack();
      onFinishedCollectException(mysql, e);
      onException(mysql, e);
      callBackAndReset.onFinishedException(e, this, null);
    }
  }

  @Override
  default void onFinishedCollect(MySQLClientSession mysql) {

  }

  @Override
  default void onException(MySQLClientSession session, Exception e) {
    MycatMonitor.onResultSetException(session, e);
    logger.error("{}", e);
    onClear(session);
    session.close(false, e);
  }

  @Override
  default void onFinishedCollectException(MySQLClientSession mysql, Exception exception) {

  }
}

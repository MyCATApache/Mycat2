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
package io.mycat.proxy.packet;

import static io.mycat.proxy.packet.MySQLPayloadType.BINARY_ROW;
import static io.mycat.proxy.packet.MySQLPayloadType.COLUMN_COUNT;
import static io.mycat.proxy.packet.MySQLPayloadType.COLUMN_DEF;
import static io.mycat.proxy.packet.MySQLPayloadType.COLUMN_EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.FIRST_EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.FIRST_ERROR;
import static io.mycat.proxy.packet.MySQLPayloadType.FIRST_OK;
import static io.mycat.proxy.packet.MySQLPayloadType.LOAD_DATA_REQUEST;
import static io.mycat.proxy.packet.MySQLPayloadType.PREPARE_OK;
import static io.mycat.proxy.packet.MySQLPayloadType.PREPARE_OK_COLUMN_DEF;
import static io.mycat.proxy.packet.MySQLPayloadType.PREPARE_OK_COLUMN_DEF_EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.PREPARE_OK_PARAMER_DEF;
import static io.mycat.proxy.packet.MySQLPayloadType.PREPARE_OK_PARAMER_DEF_EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.REQUEST;
import static io.mycat.proxy.packet.MySQLPayloadType.REQUEST_COM_STMT_CLOSE;
import static io.mycat.proxy.packet.MySQLPayloadType.REQUEST_SEND_LONG_DATA;
import static io.mycat.proxy.packet.MySQLPayloadType.ROW_EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.ROW_ERROR;
import static io.mycat.proxy.packet.MySQLPayloadType.ROW_FINISHED;
import static io.mycat.proxy.packet.MySQLPayloadType.ROW_OK;
import static io.mycat.proxy.packet.MySQLPayloadType.TEXT_ROW;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.mysql.packet.EOFPacket;
import io.mycat.beans.mysql.packet.OkPacket;
import io.mycat.beans.mysql.packet.PreparedOKPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.buffer.ProxyBuffer;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 报文处理类 该类实现报文解析
 *
 * @author jamie12221 chenjunwen design 294712221@qq.com date 2019-05-07 21:23
 **/
public interface MySQLPacketResolver extends OkPacket, EOFPacket, PreparedOKPacket {

  Logger logger = LoggerFactory.getLogger(MySQLPacketResolver.class);

  /**
   * 判断一个结果集结束时候,eof/ok 包 是否后续还有结果集
   */
  static boolean hasMoreResult(int serverStatus) {
    return MySQLServerStatusFlags.statusCheck(serverStatus, MySQLServerStatusFlags.MORE_RESULTS);
  }

  /**
   * 指示当前游标仍有结果
   */
  static boolean hasFatch(int serverStatus) {
    // 检查是否通过fatch执行的语句
    return MySQLServerStatusFlags.statusCheck(serverStatus, MySQLServerStatusFlags.CURSOR_EXISTS);
  }

  /**
   * A transaction is currently active
   */
  static boolean hasTrans(int serverStatus) {
    return MySQLServerStatusFlags.statusCheck(serverStatus, MySQLServerStatusFlags.IN_TRANSACTION)
               || MySQLServerStatusFlags
                      .statusCheck(serverStatus, MySQLServerStatusFlags.IN_TRANS_READONLY);
  }

  default boolean isErrorPacket() {
    return getHead() == 0xff;
  }

  /**
   * 获得payload类型
   */
  MySQLPayloadType getMySQLPayloadType();

  /**
   * 内部api 设置payload类型
   */
  void setMySQLPayloadType(MySQLPayloadType type);

  /**
   * 设置报文序列号
   */
  int setPacketId(int packetId);

  /**
   * 获得报文序列号
   */
  int getPacketId();

  /**
   * 未实现
   */
  boolean readFromChannel() throws IOException;

  /**
   * 未实现
   */
  void writeToChannel() throws IOException;

  /**
   * 是否已经识别出payload类型 该标志用于避免重复识别导致一些报文计数错误
   */
  boolean hasResolvePayloadType();

  /**
   * 内部api 标记报文已经被识别
   */
  void markedResolvePayloadType(boolean marked);

  /**
   * 前端请求报文payload的第一个字节,该字节标记了命令类型
   */
  int getHead();

  /**
   * 内部api,设置head
   */
  int setHead(int head);

  int getStartPos();

  /**
   * 一些报文类型要用到sql解析的sql类型辅助识别,此api就是提供这个sql类型
   */
  int setCurrentComQuerySQLType(int type);

  void setIsClientLoginRequest(boolean flag);

  boolean isClientLogin();

  int getEndPos();

  /**
   * 内部api 获取当前的sql类型
   */
  int getCurrentSQLType();

  /**
   * 透传的数据,开始下标
   */
  int setStartPos(int i);

  /**
   * 透传的数据,结束下标
   */
  int setEndPos(int i);

  /**
   * 报文解析状态
   */
  ComQueryState getState();

  /**
   * 内部api.更新解析状态
   */
  void setState(ComQueryState state);

  int getColumnCount();

  /**
   * 更新语句,结果集,使用此函数使解析器就绪
   */
  default void prepareReveiceResponse() {
    setState(ComQueryState.FIRST_PACKET);
  }

  /**
   * 预处理语句Prepare.使用此函数使解析器就绪(处理PrepareOk报文)
   */
  default void prepareReveicePrepareOkResponse() {
    setState(ComQueryState.FIRST_PACKET);
    setCurrentComQuerySQLType(0x22);
  }

  void setCapabilityFlags(int serverCapability);

  /**
   * 设置字段数量
   */
  int setColumnCount(int count);

  int getRemainsBytes();

  /**
   * 客户端是否禁用EOF报文
   */
  boolean clientDeprecateEof();

  void setPayloadLength(int length);

  /**
   * 与客户端协商得到的服务器能力标志
   */
  int capabilityFlags();

  boolean isMultiPacket();

  /**
   * 单个报文使用字数计算时候的剩余字节数
   */
  int setRemainsBytes(int remainsBytes);

  default void setRequestFininshed(boolean b) {
    setState(b ? ComQueryState.FIRST_PACKET : ComQueryState.QUERY_PACKET);
  }

  /**
   * 统计的payload长度
   */
  int getPayloadLength();

  /**
   * 是否一个payload对应多个报文
   */
  boolean setMultiPacket(boolean c);

  /**
   * 是否请求已经结束,使用该函数时候,需要同一个解析器处理请求和响应,以完成整个状态流转
   */
  default boolean isRequestFininshed() {
    return getState() != ComQueryState.QUERY_PACKET;
  }

  boolean setPayloadFinished(boolean b);

  /**
   * 重置buffer
   */
  void resetCurrentMySQLPacket();

  /**
   * 重置解析器,但是不清除payload类型以及serverStatus,因为后续操作可能用到这些信息 会清除Proxybuffer内部的buffer,但是不会清除proxybuffer,proxybuffer的引用本身在session里
   */
  default void reset() {
    resetPayload();
    resetCurrentMySQLPacket();
    if (currentProxybuffer() != null) {
      currentProxybuffer().reset();
    }
    // setPacketId((byte) -1);
    markedResolvePayloadType(false);
    setHead(0);
    setStartPos(0);
    setEndPos(0);
    setCurrentComQuerySQLType(0);
    setColumnCount(0);
    setRemainsBytes(0);
    setMultiPacket(false);

    /**
     setErrorStage(0);
     setErrorMaxStage(0);
     setErrorProgress(0);
     setErrorProgressInfo(null);
     setErrorMark((byte) 0);
     setErrorSqlState(null);
     setErrorMessage(null);
     */

    setPreparedOkStatementId(0);
    setPrepareOkColumnsCount(0);
    setPrepareOkParametersCount(0);
    setPreparedOkWarningCount(0);

    setOkAffectedRows(0);
    setOkLastInsertId(0);
    //setServerStatus(0);//后续可能存在事务的查询
//    setOkWarningCount(0);
    setOkStatusInfo(null);
    setOkSessionStateInfoType((byte) 0);
    setOkSessionStateInfoTypeData(null);
    setOkMessage(null);
//    setWarningCount(0);//存在客户端根据此警告数获取详细的警告信息的情况,所以暂时决定不清除
    //setEofServerStatus(0);
  }

  /**
   * payload是否接收结束
   */
  boolean isPayloadFinished();

  /**
   * 把payload读取完整,之后从currentPayload读取,使用resetCurrentPayload释放
   *
   * @return 是否读取完整
   */
  default boolean readMySQLPayloadFully() {
    MySQLPacket proxybuffer = currentProxybuffer();
    boolean lastIsMultiPacket = isMultiPacket();
    boolean b = readMySQLPacketFully();
    if (!b) {
      return false;
    }
    boolean multiPacket = isMultiPacket();
    int payloadStartIndex = getStartPos() + 4;
    int payloadEndIndex = getEndPos();
    if (!multiPacket && !lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return true;
    } else if (multiPacket && !lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return false;
    } else if (!multiPacket && lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return true;
    } else if (multiPacket && lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return false;
    }
    throw new MycatExpection("UNKNOWN STATE");
  }

  /**
   * 把报文读取完整
   */
  default boolean readMySQLPacketFully() {
    MySQLPacket mySQLPacket = currentProxybuffer();
    int startIndex = mySQLPacket.packetReadStartIndex();
    int endIndex = mySQLPacket.packetReadEndIndex();

    int wholePakcetSize = endIndex - startIndex;
    if (wholePakcetSize < 4) {
      return false;
    }
    int length = (int) mySQLPacket.getFixInt(startIndex, 3);
    int packetId = mySQLPacket.getByte(startIndex + 3) & 0xff;
    int andIncrementPacketId = getPacketId() + 1;
    setPacketId(packetId);
    boolean multiPacket = length == 0xffffff - 1;

    boolean isFirstPacket = false;
    boolean isEnd = !multiPacket;
    int packetLength = length;
    packetLength = packetLength + 4;
    int endPos = startIndex + packetLength;
    if (mySQLPacket.currentBuffer().capacity() < packetLength) {
      ((ProxyBuffer) mySQLPacket).expendToLengthIfNeedInReading(endPos);
    }
    if (packetLength <= (wholePakcetSize)) {
      setStartPos(startIndex);
      setEndPos(endPos);
      if (!isMultiPacket()) {
        if (length > 0) {
          int aByte = mySQLPacket.getByte(startIndex + 4) & 0xff;
          setHead(aByte);
          markedResolvePayloadType(false);
          isFirstPacket = true;
          setPayloadLength(length);
        }
      } else {
        setPayloadLength(getPayloadLength() + length);
      }
      setMultiPacket(multiPacket);

      setPayloadFinished(isEnd && getRemainsBytes() == 0);
      if (isEnd) {
        resolvePayloadType(getHead(), true, true, currentProxybuffer(), getPayloadLength());
      }
      return true;
    } else {
      setPayloadFinished(false);
      return false;
    }
  }

  /**
   * packet++;
   */
  default int getAndIncrementPacketId() {
    int packetId = getPacketId();
    byte i = (byte) (packetId + 1);
    setPacketId(i);
    return packetId;
  }

  /**
   * ++packet
   */
  default int incrementPacketIdAndGet() {
    int packetId = getPacketId();
    byte i = (byte) (packetId + 1);
    setPacketId(i);
    return i;
  }

  /**
   * 尽可能只要能识别出报文类型,返回值就是true
   */
  default boolean readMySQLPacket() throws IOException {
    MySQLPacket mySQLPacket = currentProxybuffer();
    boolean needWholePacket = getState().isNeedFull();
    if (needWholePacket && isPayloadFinished()) {
      return readMySQLPacketFully();
    } else {
      int startIndex = mySQLPacket.packetReadStartIndex();
      int endIndex = mySQLPacket.packetReadEndIndex();
      int receiveSize = endIndex - startIndex;
      if (receiveSize == 0) {
        return false;
      }
      int remains = getRemainsBytes();
      boolean lastMultiPacket = isMultiPacket();
      boolean multiPacket;
      boolean isPacketHeader = false;
      if (remains == 0) {
        if (receiveSize < 4) {
          return false;
        }
        int packetLength = (int) mySQLPacket.getFixInt(startIndex, 3);
        int packetId = mySQLPacket.getByte(startIndex + 3) & 0xff;
        if (packetLength == 0 && receiveSize >= 4) {
          setRemainsBytes(0);
          setPayloadLength(0);
          setMultiPacket(false);
          setStartPos(startIndex);
          setEndPos(startIndex + 3);
          setPacketId(packetId);
          return true;
        }
        if (receiveSize < 5) {
          return false;
        }
        int aByte = mySQLPacket.getByte(startIndex + 4) & 0xff;
        setHead(aByte);
        if (aByte == 0xfe || aByte == 0x00) {
          return readMySQLPacketFully();
        }

        setPayloadLength(packetLength);
        multiPacket = setMultiPacket(packetLength == 0xffffff - 1);
        packetLength = packetLength + 4;
        markedResolvePayloadType(false);
        setPacketId(packetId);
        int andIncrementPacketId = getAndIncrementPacketId();
        if (packetId != andIncrementPacketId) {
          throw new MycatExpection(
              "packetId :" + packetId + " andIncrementPacketId:" + andIncrementPacketId);
        }

        isPacketHeader = true;
        if (packetLength < receiveSize) {
          setStartPos(startIndex);
          setEndPos(startIndex + packetLength);
          remains = 0;
        } else {
          remains = packetLength - receiveSize;
          setStartPos(startIndex);
          setEndPos(startIndex + receiveSize);
        }
      } else {
        if (receiveSize >= remains) {
          setStartPos(startIndex);
          setEndPos(startIndex + remains);
          remains = 0;
        } else {
          remains -= receiveSize;
          setStartPos(startIndex);
          setEndPos(startIndex + receiveSize);
        }
        multiPacket = isMultiPacket();
      }
      setRemainsBytes(remains);
      boolean isEnd = !multiPacket;
      setPayloadFinished(isEnd && remains == 0);
      if (isEnd) {
        resolvePayloadType(getHead(), isPayloadFinished(), true, currentProxybuffer(),
            getPayloadLength());
      }
      return true;
    }
  }

  /**
   * 当前的buffer
   */
  MySQLPacket currentProxybuffer();

  /**
   * 把多个报文拼接成完整的Payload
   */
  void appendPayload(MySQLPacket mySQLPacket, int payloadStartIndex, int payloadEndIndex);

  /**
   * 设置Payload
   */
  void setPayload(MySQLPacket mySQLPacket);

  /**
   * 清除保存Payload的数据
   */
  void resetPayload();

  /**
   * 当前的Payload
   */
  MySQLPacket currentPayload();

  /**
   * 识别报文类型
   *
   * @param head 可能用到的信息之一,命令报文的第一个字节
   * @param isPacketFinished 报文是否接收结束
   * @param parse 是否对报文进行解析,未实现
   * @param mySQLPacket 报文本身
   * @param payloadLength Payload长度
   */
  default void resolvePayloadType(int head, boolean isPacketFinished, boolean parse,
      MySQLPacket mySQLPacket, int payloadLength) {
    if (hasResolvePayloadType()) {
      return;
    } else {
      markedResolvePayloadType(true);
    }
    switch (getState()) {
      case QUERY_PACKET: {
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        }
        if (head == 18) {
          int statementId = (int) mySQLPacket.readFixInt(4);
          int paramId = (int) mySQLPacket.readFixInt(2);
          setState(ComQueryState.QUERY_PACKET);
          setMySQLPayloadType(REQUEST_SEND_LONG_DATA);
          return;
        } else if (head == 25) {
          setState(ComQueryState.QUERY_PACKET);
          setRequestFininshed(true);
          setMySQLPayloadType(REQUEST_COM_STMT_CLOSE);
          return;
        } else {
          setCurrentComQuerySQLType(head);
          setState(ComQueryState.FIRST_PACKET);
          setRequestFininshed(true);
          setMySQLPayloadType(REQUEST);
          return;
        }
      }
      case AUTH_SWITCH_PLUGIN_RESPONSE:
      case AUTH_SWITCH_OTHER_REQUEST:
      case FIRST_PACKET: {
        if (!isPacketFinished) {
          throw new MycatExpection("unknown state!");
        }
        if (head == 0xff) {
          setState(ComQueryState.COMMAND_END);
          setMySQLPayloadType(FIRST_ERROR);
        } else if (head == 0x00) {
          if (getCurrentSQLType() == 0x22 && payloadLength == 12 && getPacketId() == 2) {
            resolvePrepareOkPacket(mySQLPacket, isPacketFinished);
            setMySQLPayloadType(PREPARE_OK);
            return;
          } else {
            setServerStatus(okPacketReadServerStatus(mySQLPacket));
            setMySQLPayloadType(FIRST_OK);
            if (hasMoreResult(getServerStatus())) {
              setState(ComQueryState.FIRST_PACKET);
            } else {
              setState(ComQueryState.COMMAND_END);
            }
            return;
          }
        } else if (head == 0xfb) {
          setState(ComQueryState.LOCAL_INFILE_FILE_CONTENT);
          setMySQLPayloadType(LOAD_DATA_REQUEST);
          return;
        } else if (head == 0xfe) {
          if (isClientLogin()) {
            setMySQLPayloadType(FIRST_EOF);
            setState(ComQueryState.AUTH_SWITCH_PLUGIN_RESPONSE);
          } else {
            setServerStatus(eofPacketReadStatus(mySQLPacket));
            setState(ComQueryState.COMMAND_END);
            setMySQLPayloadType(FIRST_EOF);
          }
          return;
        } else {
          if (isClientLogin()) {
            setState(ComQueryState.AUTH_SWITCH_OTHER_REQUEST);
            return;
          }
          int count = (int) mySQLPacket
                                .getLenencInt(getStartPos() + MySQLPacket.getPacketHeaderSize());
          setColumnCount(count);
          setState(ComQueryState.COLUMN_DEFINITION);
          setMySQLPayloadType(COLUMN_COUNT);
        }
        return;
      }
      case COLUMN_DEFINITION: {
        if (setColumnCount(getColumnCount() - 1) == 0) {
          setState(
              !clientDeprecateEof() ? ComQueryState.COLUMN_END_EOF : ComQueryState.RESULTSET_ROW);
        }
        setMySQLPayloadType(COLUMN_DEF);
        return;
      }
      case COLUMN_END_EOF: {
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        }
        setServerStatus(eofPacketReadStatus(mySQLPacket));
        setState(ComQueryState.RESULTSET_ROW);
        setMySQLPayloadType(COLUMN_EOF);
        return;
      }
      case RESULTSET_ROW: {
        if (head == 0x00) {
          setMySQLPayloadType(BINARY_ROW);
        } else if (head == 0xfe && payloadLength < 0xffffff) {
          resolveResultsetRowEnd(mySQLPacket, isPacketFinished);
        } else if (head == 0xff) {
          setState(ComQueryState.COMMAND_END);
          setMySQLPayloadType(ROW_ERROR);
        } else {
          //text resultset row
          setMySQLPayloadType(TEXT_ROW);
        }
        break;
      }
//            case RESULTSET_ROW_END:
//                resolveResultsetRowEnd(currentMySQLPacket, isPacketFinished);
//                break;
      case PREPARE_FIELD:
      case PREPARE_FIELD_EOF:
      case PREPARE_PARAM:
      case PREPARE_PARAM_EOF:
        resolvePrepareResponse(mySQLPacket, head, isPacketFinished);
        return;
      case LOCAL_INFILE_FILE_CONTENT:
        if (payloadLength == 4) {
          setState(ComQueryState.LOCAL_INFILE_OK_PACKET);
          return;
        } else {
          setState(ComQueryState.LOCAL_INFILE_FILE_CONTENT);
          return;
        }
      case LOCAL_INFILE_OK_PACKET:
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        }
        setServerStatus(okPacketReadServerStatus(mySQLPacket));
        setState(ComQueryState.COMMAND_END);
        return;
      case COMMAND_END: {
      }
      return;
      default: {
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        } else {
          throw new RuntimeException("unknown state!");
        }
      }
    }
  }

  /**
   * 识别prepareOk报文
   */
  default void resolvePrepareOkPacket(MySQLPacket buffer, boolean isPacketFinished) {
    if (!isPacketFinished) {
      throw new RuntimeException("unknown state!");
    }
    int bpStartIndex = buffer.packetReadStartIndex();
    int bpEndIndex = buffer.packetReadEndIndex();
    buffer.packetReadStartIndex(getStartPos() + 4 + 1);
    buffer.packetReadEndIndex(getEndPos());
    setPreparedOkStatementId(buffer.readFixInt(4));
    setPrepareOkColumnsCount((int) buffer.readFixInt(2));
    setPrepareOkParametersCount((int) buffer.readFixInt(2));
    buffer.skipInReading(1);
    setPreparedOkWarningCount((int) buffer.readFixInt(2));
    buffer.packetReadStartIndex(bpStartIndex);
    buffer.packetReadEndIndex(bpEndIndex);
    if (this.getPrepareOkColumnsCount() == 0 && getPrepareOkParametersCount() == 0) {
      setState(ComQueryState.COMMAND_END);
      return;
    } else if (this.getPrepareOkColumnsCount() > 0) {
      setState(ComQueryState.PREPARE_FIELD);
      return;
    }
    if (getPrepareOkParametersCount() > 0) {
      setState(ComQueryState.PREPARE_PARAM);
      return;
    }
    throw new RuntimeException("unknown state!");
  }

  /**
   * 响应是否结束
   */
  default boolean isResponseFinished() {
    return getState() == ComQueryState.COMMAND_END;
  }

  /**
   * setState(b ? ComQueryState.COMMAND_END : ComQueryState.FIRST_PACKET);
   */
  default void setResponseFinished(boolean b) {
    setState(b ? ComQueryState.COMMAND_END : ComQueryState.FIRST_PACKET);
  }

  /**
   * 从eof packet读取服务器状态 该函数并不会改变buffer内部状态
   */
  default int eofPacketReadStatus(MySQLPacket buffer) {
    int bpStartIndex = buffer.packetReadStartIndex();
    int bpEndIndex = buffer.packetReadEndIndex();
    buffer.packetReadStartIndex(getStartPos());
    buffer.packetReadStartIndex(getEndPos());
    //7 = packetLength(3) +  packetId（1） +  pkgType（1） + getWarningCount（2）
//        buffers.skipInReading(7);
    setServerStatus((int) buffer.getFixInt(getStartPos() + 5, 2));
    int i = setServerStatus((int) buffer.getFixInt(getStartPos() + 7, 2));//status
    buffer.packetReadStartIndex(bpStartIndex);
    buffer.packetReadEndIndex(bpEndIndex);
    return i;
  }

  enum ComQueryState {
    //DO_NOT(false),
    QUERY_PACKET(true),
    FIRST_PACKET(true),
    COLUMN_DEFINITION(false),
    AUTH_SWITCH_PLUGIN_RESPONSE(true),
    AUTH_SWITCH_OTHER_REQUEST(true),
    COLUMN_END_EOF(true),
    RESULTSET_ROW(false),
    RESULTSET_ROW_END(true),
    PREPARE_FIELD(false),
    PREPARE_FIELD_EOF(true),
    PREPARE_PARAM(false),
    PREPARE_PARAM_EOF(true),
    COMMAND_END(false),
    //    LOCAL_INFILE_REQUEST(true),
    LOCAL_INFILE_FILE_CONTENT(true),
    //    LOCAL_INFILE_EMPTY_PACKET(true),
    LOCAL_INFILE_OK_PACKET(true);
    boolean needFull;

    ComQueryState(boolean needFull) {
      this.needFull = needFull;
    }

    public boolean isNeedFull() {
      return needFull;
    }
  }

  /**
   * 从ok packet读取状态 该函数并不会改变buffer内部状态
   */
  default int okPacketReadServerStatus(MySQLPacket buffer) {
    int bpStartIndex = buffer.packetReadStartIndex();
    int bpEndIndex = buffer.packetReadEndIndex();
    buffer.packetReadStartIndex(getStartPos() + 4);
    buffer.packetReadEndIndex(getEndPos());
    byte header = buffer.readByte();
    assert (0x00 == header) || (0xfe == header);
    int serverStatus = 0;

    setOkAffectedRows(buffer.readLenencInt());//affectedRows
    setOkLastInsertId(buffer.readLenencInt());//getLastInsertId
    int capabilityFlags = capabilityFlags();
    if (MySQLServerCapabilityFlags.isClientProtocol41(capabilityFlags) || MySQLServerCapabilityFlags
                                                                              .isKnowsAboutTransactions(
                                                                                  capabilityFlags)) {
      setServerStatus(serverStatus = (int) buffer.readFixInt(2));
      buffer.packetReadStartIndex(bpStartIndex);
      buffer.packetReadEndIndex(bpEndIndex);
      return serverStatus;
    }
    throw new java.lang.RuntimeException("OKPacket readServerStatus error ");
  }

  /**
   * 识别结果集结束的报文
   */
  default void resolveResultsetRowEnd(MySQLPacket mySQLPacket, boolean isPacketFinished) {
    if (!isPacketFinished) {
      throw new RuntimeException("unknown state!");
    }
    if (clientDeprecateEof()) {
      setServerStatus(okPacketReadServerStatus(mySQLPacket));
      setMySQLPayloadType(ROW_OK);
    } else {
      setServerStatus(eofPacketReadStatus(mySQLPacket));
      setMySQLPayloadType(ROW_EOF);
    }
    int startPos = getStartPos();
    int endPos = getEndPos();
    if (hasMoreResult(getServerStatus())) {
      setState(ComQueryState.FIRST_PACKET);
    } else {
      setState(ComQueryState.COMMAND_END);
      setMySQLPayloadType(ROW_FINISHED);
    }
  }

  /**
   * 处理预处理响应
   */
  default void resolvePrepareResponse(MySQLPacket proxyBuf, int head, boolean isPacketFinished) {
    if (!isPacketFinished) {
      throw new RuntimeException("unknown state!");
    }
    PreparedOKPacket p = this;
    if (p.getPrepareOkColumnsCount() > 0 && (getState() == ComQueryState.PREPARE_FIELD)) {
      p.setPrepareOkColumnsCount(p.getPrepareOkColumnsCount() - 1);
      setState(ComQueryState.PREPARE_FIELD);
      setMySQLPayloadType(PREPARE_OK_COLUMN_DEF);
      if (p.getPrepareOkColumnsCount() == 0) {
        if (!clientDeprecateEof()) {
          setState(ComQueryState.PREPARE_FIELD_EOF);
        } else if (p.getPrepareOkParametersCount() > 0) {
          setState(ComQueryState.PREPARE_PARAM);
        } else {
          setState(ComQueryState.COMMAND_END);
        }
      }
      return;
    } else if (getState() == ComQueryState.PREPARE_FIELD_EOF && head == 0xFE
                   && p.getPrepareOkParametersCount() > 0) {
      setServerStatus(eofPacketReadStatus(proxyBuf));
      setState(ComQueryState.PREPARE_PARAM);
      setMySQLPayloadType(PREPARE_OK_COLUMN_DEF_EOF);
      return;
    } else if (getState() == ComQueryState.PREPARE_FIELD_EOF && head == 0xFE
                   && p.getPrepareOkParametersCount() == 0) {
      setServerStatus(eofPacketReadStatus(proxyBuf));
      setState(ComQueryState.COMMAND_END);
      setMySQLPayloadType(PREPARE_OK_COLUMN_DEF_EOF);
      return;
    } else if (p.getPrepareOkParametersCount() > 0 && getState() == ComQueryState.PREPARE_PARAM) {
      p.setPrepareOkParametersCount(p.getPrepareOkParametersCount() - 1);
      setState(ComQueryState.PREPARE_PARAM);
      setMySQLPayloadType(PREPARE_OK_PARAMER_DEF);
      if (p.getPrepareOkParametersCount() == 0) {
        if (!clientDeprecateEof()) {
          setState(ComQueryState.PREPARE_PARAM_EOF);
          return;
        } else {
          setState(ComQueryState.COMMAND_END);
          return;
        }
      } else {
        return;
      }
    } else if (p.getPrepareOkColumnsCount() == 0 && p.getPrepareOkParametersCount() == 0
                   && !clientDeprecateEof() && ComQueryState.PREPARE_PARAM_EOF == getState()
                   && head == 0xFE) {
      setServerStatus(eofPacketReadStatus(proxyBuf));
      setState(ComQueryState.COMMAND_END);
      setMySQLPayloadType(PREPARE_OK_PARAMER_DEF_EOF);
      return;
    }
    throw new RuntimeException("unknown state!");
  }

}

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
package io.mycat.proxy.packet;

import static io.mycat.proxy.packet.ComQueryState.COLUMN_DEFINITION;
import static io.mycat.proxy.packet.ComQueryState.FIRST_PACKET;
import static io.mycat.proxy.packet.ComQueryState.QUERY_PACKET;
import static io.mycat.proxy.packet.MySQLPacketProcessType.BINARY_ROW;
import static io.mycat.proxy.packet.MySQLPacketProcessType.COLUMN_COUNT;
import static io.mycat.proxy.packet.MySQLPacketProcessType.COLUMN_DEF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.COLUMN_EOF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.FIRST_EOF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.FIRST_ERROR;
import static io.mycat.proxy.packet.MySQLPacketProcessType.FIRST_OK;
import static io.mycat.proxy.packet.MySQLPacketProcessType.LOAD_DATA_REQUEST;
import static io.mycat.proxy.packet.MySQLPacketProcessType.PREPARE_OK;
import static io.mycat.proxy.packet.MySQLPacketProcessType.PREPARE_OK_COLUMN_DEF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.PREPARE_OK_COLUMN_DEF_EOF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.PREPARE_OK_PARAMER_DEF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.PREPARE_OK_PARAMER_DEF_EOF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.REQUEST;
import static io.mycat.proxy.packet.MySQLPacketProcessType.REQUEST_COM_STMT_CLOSE;
import static io.mycat.proxy.packet.MySQLPacketProcessType.REQUEST_SEND_LONG_DATA;
import static io.mycat.proxy.packet.MySQLPacketProcessType.ROW_EOF;
import static io.mycat.proxy.packet.MySQLPacketProcessType.ROW_ERROR;
import static io.mycat.proxy.packet.MySQLPacketProcessType.ROW_FINISHED;
import static io.mycat.proxy.packet.MySQLPacketProcessType.ROW_OK;
import static io.mycat.proxy.packet.MySQLPacketProcessType.TEXT_ROW;
import static io.mycat.proxy.packet.MySQLPayloadType.EOF;
import static io.mycat.proxy.packet.MySQLPayloadType.LOCAL_INFILE_CONTENT_OF_FILENAME;
import static io.mycat.proxy.packet.MySQLPayloadType.LOCAL_INFILE_EMPTY_PACKET;
import static io.mycat.proxy.packet.MySQLPayloadType.LOCAL_INFILE_REQUEST;
import static io.mycat.proxy.packet.MySQLPayloadType.UNKNOWN;

import io.mycat.beans.mysql.MySQLCapabilityFlags;
import io.mycat.beans.mysql.MySQLServerStatus;
import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.util.StringUtil;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MySQLPacketResolver extends OkPacket, EOFPacket, ErrorPacket, PreparedOKPacket {

  static final Logger logger = LoggerFactory.getLogger(MySQLPacketResolver.class);

  MySQLPacketProcessType getMySQLPacketProcessType();

  void setMySQLPacketProcessType(MySQLPacketProcessType type);

  public void setWholePacketStartPos(int length);

  public void setWholePacketEndPos(int length);

  public int getWholePacketStartPos();

  public int getWholePacketEndPos();

  public void setPayloadLength(int length);

  public int getPayloadLength();

  int setPacketId(int packetId);

  int getPacketId();

  boolean readFromChannel() throws IOException;

  void writeToChannel() throws IOException;

  boolean hasResolvePayloadType();

  void markedResolvePayloadType(boolean marked);

  int getHead();

  int setHead(int head);

  int setCurrentComQuerySQLType(int type);

  int getCurrentSQLType();

  int setStartPos(int i);

  int getStartPos();

  int setEndPos(int i);

  int getEndPos();

  int setPayloadStartPos(int i);

  int getPayloadStartPos();

  int setPayloadEndPos(int i);

  int getPayloadEndPos();

  ComQueryState getState();

  void setState(ComQueryState state);

  void setMySQLpayloadType(MySQLPayloadType type);

  int setColumnCount(int count);

  int getColumnCount();

  boolean clientDeprecateEof();

  MySQLCapabilityFlags capabilityFlags();

  int setRemainsBytes(int remainsBytes);

  int getRemainsBytes();

  public boolean setMultiPacket(boolean c);

  public boolean isMultiPacket();

  public MySQLPacket currentProxybuffer();

  public void appendPayload(MySQLPacket mySQLPacket, int payloadStartIndex, int payloadEndIndex);

  public void setPayload(MySQLPacket mySQLPacket);

   public void resetPayload() ;

  public MySQLPacket currentPayload();

  default void setRequestFininshed(boolean b) {
    setState(b ? FIRST_PACKET : QUERY_PACKET);
  }

  default boolean isRequestFininshed() {
    return getState() != QUERY_PACKET;
  }

  void resetCurrentMySQLPacket();

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
    setMySQLpayloadType(UNKNOWN);
    setColumnCount(0);
    setRemainsBytes(0);
    setMultiPacket(false);
    setErrorStage(0);
    setErrorMaxStage(0);
    setErrorProgress(0);
    setErrorProgressInfo(null);
    setErrorMark((byte) 0);
    setErrorSqlState(null);
    setErrorMessage(null);
    setPreparedOkStatementId(0);
    setPrepareOkColumnsCount(0);
    setPrepareOkParametersCount(0);
    setPreparedOkWarningCount(0);

    setOkAffectedRows(0);
    setOkLastInsertId(0);
    setOkServerStatus(0);
    setOkWarningCount(0);
    setOkStatusInfo(null);
    setOkSessionStateInfoType((byte) 0);
    setOkSessionStateInfoTypeData(null);
    setOkMessage(null);
    setEofWarningCount(0);
    setEofServerStatus(0);
  }

  boolean isPayloadFinished();

  boolean setPayloadFinished(boolean b);


  default boolean readMySQLPayloadFully() throws IOException {
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
      setWholePacketStartPos(getStartPos());
      setWholePacketEndPos(getEndPos());

      setPayloadStartPos(payloadStartIndex);
      setPayloadEndPos(getEndPos());
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return true;
    }
    if (multiPacket && !lastIsMultiPacket) {
      setWholePacketStartPos(getStartPos());
      setPayloadStartPos(payloadStartIndex);
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      return false;
    }
    if (!multiPacket && lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      setEndPos(payloadEndIndex);
      setWholePacketEndPos(payloadEndIndex);
      setPayloadEndPos(payloadEndIndex);
    }
    if (multiPacket && lastIsMultiPacket) {
      appendPayload(currentProxybuffer(), payloadStartIndex, payloadEndIndex);
      setEndPos(payloadEndIndex);
    }
    return true;
  }

  default boolean readMySQLPacketFully() throws IOException {
    logger.debug("readMySQLPacketFully");
    MySQLPacket mySQLPacket = currentProxybuffer();
    int startIndex = mySQLPacket.packetReadStartIndex();
    int endIndex = mySQLPacket.packetReadEndIndex();
    logger.debug("startIndex:" + startIndex);
    logger.debug("endIndex:" + endIndex);
    logger.debug(Objects.toString(this.getState()));
    logger.debug(Objects.toString(this.getPailoadType()));
    int wholePakcetSize = endIndex - startIndex;
    if (wholePakcetSize < 4) {
      return false;
    }
    System.out.print(StringUtil.dumpAsHex(mySQLPacket.currentBuffer().currentByteBuffer(),0,128));
    int length = (int) mySQLPacket.getFixInt(startIndex, 3);
    if (length>1000){
      System.out.println();
    }
    logger.debug("body:" + length);
    int packetId = mySQLPacket.getByte(startIndex + 3) & 0xff;
    int andIncrementPacketId = getPacketId() + 1;
    setPacketId(packetId);
    boolean multiPacket = length == 0xffffff - 1;

    boolean isFirstPacket = false;
    boolean isEnd = !multiPacket;
    int packetLength = length;
    if (multiPacket) {
      packetLength = 0xffffff - 1;
    } else {
      packetLength = packetLength + 4;
    }
    ((ProxyBuffer) mySQLPacket).expendToLengthIfNeedInReading(startIndex + packetLength);
    if (packetLength <= (wholePakcetSize)) {
      setStartPos(startIndex);
      setEndPos(startIndex + packetLength);
      logger.debug("packetLength:" + packetLength);
      logger.debug("startPos:" + getStartPos());
      logger.debug("endPos:" + getEndPos());
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
      } else {

      }
      return isEnd;
    } else {
      setPayloadFinished(false);
      return false;
    }
  }

  default int getAndIncrementPacketId() {
    int packetId = getPacketId();
    int i = packetId + 1;
    setPacketId(i == 256 ? 0 : i);
    return packetId;
  }

  default int incrementPacketIdAndGet() {
    int packetId = getPacketId();
    int i = packetId + 1;
    return setPacketId(i == 256 ? 0 : i);
  }

  default boolean readMySQLPacket() throws IOException {
    MySQLPacket mySQLPacket = currentProxybuffer();
    boolean needWholePacket = getState().isNeedFull();
    if (needWholePacket && isPayloadFinished()) {
      System.out.println("readMySQLPacketFully");
      return readMySQLPacketFully();
    } else {
      System.out.println("readMySQLPacket");
      int startIndex = mySQLPacket.packetReadStartIndex();
      int endIndex = mySQLPacket.packetReadEndIndex();
      logger.debug("startIndex:" + startIndex);
      logger.debug("endIndex:" + endIndex);
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
        logger.debug("body:" + packetLength);
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
        int aByte;
        try {
          aByte = mySQLPacket.getByte(startIndex + 4) & 0xff;
          setHead(aByte);
          if (aByte == 0xfe || aByte == 0x00) {
            return readMySQLPacketFully();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        setPayloadLength(packetLength);
        multiPacket = setMultiPacket(packetLength == 0xffffff);
        if (multiPacket) {
          packetLength = 0xffffff;
        } else {
          packetLength = packetLength + 4;
        }
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
          logger.debug("packetLength:" + packetLength);
          logger.debug("startPos:" + getStartPos());
          logger.debug("endPos:" + getEndPos());
          remains = 0;
        } else {
          remains = packetLength - receiveSize;
          setStartPos(startIndex);
          setEndPos(startIndex + receiveSize);
          logger.debug("packetLength:" + packetLength);
          logger.debug("startPos:" + getStartPos());
          logger.debug("endPos:" + getEndPos());
          System.out.println("remains:" + remains);
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
//            if (isPacketHeader && isEnd || !lastMultiPacket && !isEnd || !lastMultiPacket && isEnd) {
//                appendFirstPacket(currentPayload(), currentMySQLPacket, getStartPos(), getEndPos(), getRemainsBytes());
//            } else if (isPacketHeader && multiPacket && !lastMultiPacket&&!isEnd) {
//                appendFirstMultiPacket(currentPayload(), currentMySQLPacket, getStartPos(), getEndPos(), getRemainsBytes());
//            } else if (lastMultiPacket&&!isPacketHeader && isEnd) {
//                appendEndMultiPacket(currentPayload(), currentMySQLPacket, getStartPos(), getEndPos(), getRemainsBytes());
//            } else if (!isPacketHeader && !isEnd) {
//                appendAfterMultiPacket(currentPayload(), currentMySQLPacket, getStartPos(), getEndPos(), getRemainsBytes());
//            }
      setPayloadFinished(isEnd && remains == 0);
      if (isEnd) {
        resolvePayloadType(getHead(), false, true, currentProxybuffer(), getPayloadLength());
      }
      return true;
    }
  }


  default public void resolvePayloadType(int head, boolean isPacketFinished, boolean parse,
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
          setMySQLpayloadType(MySQLPayloadType.SEND_LONG_DATA);
          setState(QUERY_PACKET);
          setMySQLPacketProcessType(REQUEST_SEND_LONG_DATA);
          return;
        } else if (head == 25) {
          setMySQLpayloadType(MySQLPayloadType.COM_STMT_CLOSE);
          setState(QUERY_PACKET);
          setRequestFininshed(true);
          setMySQLPacketProcessType(REQUEST_COM_STMT_CLOSE);
          return;
        } else {
          setMySQLpayloadType(MySQLPayloadType.UNKNOWN);
          setCurrentComQuerySQLType(head);
          setState(FIRST_PACKET);
          setRequestFininshed(true);
          setMySQLPacketProcessType(REQUEST);
          return;
        }
      }
      case FIRST_PACKET: {
        if (!isPacketFinished) {
          throw new MycatExpection("unknown state!");
        }
        if (head == 0xff) {
          setMySQLpayloadType(MySQLPayloadType.ERROR);
          setState(ComQueryState.COMMAND_END);
          setMySQLPacketProcessType(FIRST_ERROR);
        } else if (head == 0x00) {
          if (getCurrentSQLType() == 0x22 && payloadLength == 12 && getPacketId() == 2) {
            resolvePrepareOkPacket(mySQLPacket, isPacketFinished);
            setMySQLPacketProcessType(PREPARE_OK);
            return;
          } else {
            setMySQLpayloadType(MySQLPayloadType.OK);
            setOkServerStatus(okPacketReadServerStatus(mySQLPacket));
            setMySQLPacketProcessType(FIRST_OK);
            if (hasMoreResult(getOkServerStatus())) {
              setState(FIRST_PACKET);
            } else {
              setState(ComQueryState.COMMAND_END);
            }
            return;
          }
        } else if (head == 0xfb) {
          setState(ComQueryState.LOCAL_INFILE_FILE_CONTENT);
          setMySQLpayloadType(LOCAL_INFILE_REQUEST);
          setMySQLPacketProcessType(LOAD_DATA_REQUEST);
          return;
        } else if (head == 0xfe) {
          setMySQLpayloadType(EOF);
          setOkServerStatus(eofPacketReadStatus(mySQLPacket));
          setState(ComQueryState.COMMAND_END);
          setMySQLPacketProcessType(FIRST_EOF);
          return;
        } else {
          int count =   (int) mySQLPacket.getLenencInt(getStartPos() + MySQLPacket.getPacketHeaderSize());
          setColumnCount(count);
          setMySQLpayloadType(MySQLPayloadType.COLUMN_COUNT);
          setState(COLUMN_DEFINITION);
          setMySQLPacketProcessType(COLUMN_COUNT);
        }
        return;
      }
      case COLUMN_DEFINITION: {
        if (setColumnCount(getColumnCount() - 1) == 0) {
          setState(
              !clientDeprecateEof() ? ComQueryState.COLUMN_END_EOF : ComQueryState.RESULTSET_ROW);
        }
        setMySQLpayloadType(MySQLPayloadType.COLUMN_DEFINITION);
        setMySQLPacketProcessType(COLUMN_DEF);
        return;
      }
      case COLUMN_END_EOF: {
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        }
        setOkServerStatus(eofPacketReadStatus(mySQLPacket));
        setMySQLpayloadType(EOF);
        setState(ComQueryState.RESULTSET_ROW);
        setMySQLPacketProcessType(COLUMN_EOF);
        return;
      }
      case RESULTSET_ROW: {
        if (head == 0x00) {
          setMySQLpayloadType(MySQLPayloadType.BINARY_RESULTSET_ROW);
          setMySQLPacketProcessType(BINARY_ROW);
        } else if (head == 0xfe && payloadLength < 0xffffff) {
          resolveResultsetRowEnd(mySQLPacket, isPacketFinished);
        } else if (head == 0xff) {
          setMySQLpayloadType(MySQLPayloadType.ERROR);
          setState(ComQueryState.COMMAND_END);
          setMySQLPacketProcessType(ROW_ERROR);
        } else {
          setMySQLpayloadType(MySQLPayloadType.TEXT_RESULTSET_ROW);
          //text resultset row
          setMySQLPacketProcessType(TEXT_ROW);
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
          setMySQLpayloadType(LOCAL_INFILE_EMPTY_PACKET);
          return;
        } else {
          setState(ComQueryState.LOCAL_INFILE_FILE_CONTENT);
          setMySQLpayloadType(LOCAL_INFILE_CONTENT_OF_FILENAME);
          return;
        }
      case LOCAL_INFILE_OK_PACKET:
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        }
        setMySQLpayloadType(MySQLPayloadType.OK);
        setOkServerStatus(okPacketReadServerStatus(mySQLPacket));
        setState(ComQueryState.COMMAND_END);
        return;
      case COMMAND_END: {
      }
      return;
      default: {
        if (!isPacketFinished) {
          throw new RuntimeException("unknown state!");
        } else {

        }
      }
    }
  }

  default public void setResponseFinished(boolean b) {
    setState(b ? ComQueryState.COMMAND_END : FIRST_PACKET);
  }

  default public boolean isResponseFinished() {
    return getState() == ComQueryState.COMMAND_END;
  }

  default public int eofPacketReadStatus(MySQLPacket buffer) {
    int bpStartIndex = buffer.packetReadStartIndex();
    int bpEndIndex = buffer.packetReadEndIndex();
    buffer.packetReadStartIndex(getStartPos());
    buffer.packetReadStartIndex(getEndPos());
    //7 = packetLength(3) +  packetId（1） +  pkgType（1） + warningCount（2）
//        buffers.skipInReading(7);
    setEofServerStatus((int) buffer.getFixInt(getStartPos() + 5, 2));
    int i = setOkServerStatus((int) buffer.getFixInt(getStartPos() + 7, 2));//status
    buffer.packetReadStartIndex(bpStartIndex);
    buffer.packetReadEndIndex(bpEndIndex);
    return i;
  }

  default public int okPacketReadServerStatus(MySQLPacket buffer) {
    int bpStartIndex = buffer.packetReadStartIndex();
    int bpEndIndex = buffer.packetReadEndIndex();
    buffer.packetReadStartIndex(getStartPos() + 4);
    buffer.packetReadEndIndex(getEndPos());
    byte header = buffer.readByte();
    assert (0x00 == header) || (0xfe == header);
    int serverStatus = 0;

    setOkAffectedRows(buffer.readLenencInt());//affectedRows
    setOkLastInsertId(buffer.readLenencInt());//lastInsertId
    MySQLCapabilityFlags capabilityFlags = capabilityFlags();
    if (capabilityFlags.isClientProtocol41() || capabilityFlags.isKnowsAboutTransactions()) {
      setOkServerStatus(serverStatus = (int) buffer.readFixInt(2));
      buffer.packetReadStartIndex(bpStartIndex);
      buffer.packetReadEndIndex(bpEndIndex);
      return serverStatus;
    }
    throw new java.lang.RuntimeException("OKPacket readServerStatus error ");
  }
//
//    default public void okPacketReadPayload(MySQLPacket buffers) {
//        byte header = buffers.readByte();
//        assert (0x00 == header) || (0xfe == header);
//        setOkAffectedRows(buffers.readLenencInt());
//        setOkLastInsertId(buffers.readLenencInt());
//
//        MySQLCapabilityFlags capabilityFlags = capabilityFlags();
//        if (capabilityFlags.isClientProtocol41()) {
//            setOkServerStatus((int) buffers.readFixInt(2));
//            setOkWarningCount((int) buffers.readFixInt(2));
//
//        } else if (capabilityFlags.isKnowsAboutTransactions()) {
//            setOkServerStatus((int) buffers.readFixInt(2));
//        }
//        if (capabilityFlags.isSessionVariableTracking()) {
//            setOkStatusInfo(buffers.readLenencBytes());
//            if ((getOkServerStatus() & MySQLServerStatus.STATE_CHANGED) != 0) {
//                setOkSessionStateInfoType(buffers.readByte());
//                setOkSessionStateInfoTypeData(buffers.readLenencBytes());
//            }
//        } else {
//            setOkMessage(buffers.readEOFStringBytes());
//        }
//    }

  default void resolveResultsetRowEnd(MySQLPacket mySQLPacket, boolean isPacketFinished) {
    if (!isPacketFinished) {
      throw new RuntimeException("unknown state!");
    }
    logger.debug("{}", getPacketId());
    if (clientDeprecateEof()) {
      setMySQLpayloadType(MySQLPayloadType.OK);
      setOkServerStatus(okPacketReadServerStatus(mySQLPacket));
      setMySQLPacketProcessType(ROW_OK);
    } else {
      setMySQLpayloadType(EOF);
      setOkServerStatus(eofPacketReadStatus(mySQLPacket));
      setMySQLPacketProcessType(ROW_EOF);
    }
    int startPos = getStartPos();
    int endPos = getEndPos();
    logger.debug("{}", endPos - startPos);
    logger.debug("{}", getPacketId());
    if (hasMoreResult(getOkServerStatus())) {
      setState(FIRST_PACKET);
    } else {
      setState(ComQueryState.COMMAND_END);
      setMySQLPacketProcessType(ROW_FINISHED);
    }
  }

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
    setMySQLpayloadType(MySQLPayloadType.PREPARE_OK);
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

  default void resolvePrepareResponse(MySQLPacket proxyBuf, int head, boolean isPacketFinished) {
    if (!isPacketFinished) {
      throw new RuntimeException("unknown state!");
    }
    PreparedOKPacket p = this;
    if (p.getPrepareOkColumnsCount() > 0 && (getState() == ComQueryState.PREPARE_FIELD)) {
      p.setPrepareOkColumnsCount(p.getPrepareOkColumnsCount() - 1);
      setMySQLpayloadType(MySQLPayloadType.COLUMN_DEFINITION);
      setState(ComQueryState.PREPARE_FIELD);
      setMySQLPacketProcessType(PREPARE_OK_COLUMN_DEF);
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
      setOkServerStatus(eofPacketReadStatus(proxyBuf));
      setMySQLpayloadType(EOF);
      setState(ComQueryState.PREPARE_PARAM);
      setMySQLPacketProcessType(PREPARE_OK_COLUMN_DEF_EOF);
      return;
    } else if (getState() == ComQueryState.PREPARE_FIELD_EOF && head == 0xFE
                   && p.getPrepareOkParametersCount() == 0) {
      setOkServerStatus(eofPacketReadStatus(proxyBuf));
      setMySQLpayloadType(EOF);
      setState(ComQueryState.COMMAND_END);
      setMySQLPacketProcessType(PREPARE_OK_COLUMN_DEF_EOF);
      return;
    } else if (p.getPrepareOkParametersCount() > 0 && getState() == ComQueryState.PREPARE_PARAM) {
      p.setPrepareOkParametersCount(p.getPrepareOkParametersCount() - 1);
      setMySQLpayloadType(MySQLPayloadType.COLUMN_DEFINITION);
      setState(ComQueryState.PREPARE_PARAM);
      setMySQLPacketProcessType(PREPARE_OK_PARAMER_DEF);
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
      setOkServerStatus(eofPacketReadStatus(proxyBuf));
      setMySQLpayloadType(EOF);
      setState(ComQueryState.COMMAND_END);
      setMySQLPacketProcessType(PREPARE_OK_PARAMER_DEF_EOF);
      return;
    }
    throw new RuntimeException("unknown state!");
  }

  public static boolean hasMulitQuery(int serverStatus) {
    return MySQLServerStatus.statusCheck(serverStatus, MySQLServerStatus.MULIT_QUERY);
  }

  public static boolean hasMoreResult(int serverStatus) {
    return MySQLServerStatus.statusCheck(serverStatus, MySQLServerStatus.MORE_RESULTS);
  }

  public static boolean hasResult(int serverStatus) {
    return (hasMoreResult(serverStatus) || hasMulitQuery(serverStatus));
  }

  public static boolean hasFatch(int serverStatus) {
    // 检查是否通过fatch执行的语句
    return MySQLServerStatus.statusCheck(serverStatus, MySQLServerStatus.CURSOR_EXISTS);
  }

  public static boolean hasTrans(int serverStatus) {
    // 检查是否通过fatch执行的语句
    boolean trans = MySQLServerStatus.statusCheck(serverStatus, MySQLServerStatus.IN_TRANSACTION)
                        || MySQLServerStatus
                               .statusCheck(serverStatus, MySQLServerStatus.IN_TRANS_READONLY);
    return trans;
  }


  MySQLPayloadType getPailoadType();
}

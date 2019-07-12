package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.MySQLPacketUtil;
import java.nio.charset.Charset;

public interface MySQLServerSession<T extends Session<T>> extends Session<T> {



  /**
   * 设置上下文packetId,用于响应生成
   */
  void setPakcetId(int packet);

  /**
   * ++packetId
   */
  byte getNextPacketId();

  /**
   * 获取 上下文错误信息,用于构造error Packet
   */
  String getLastMessage();

  /**
   * ok packet
   */
  long affectedRows();

  int setServerStatus(int s);

  int incrementWarningCount();

  /**
   * ok packet
   */
  long incrementAffectedRows();

  /**
   * ok eof
   */
  int getServerStatus();

  int setLastInsertId(int s);

  int getWarningCount();

  boolean isDeprecateEOF();

  long getLastInsertId();

  int getLastErrorCode();

  void setLastErrorCode(int errorCode);



  /**
   * 与客户端协商的服务器能力
   */
  int getCapabilities();

  /**
   * 前端写入事件是否结束
   */
  boolean isResponseFinished();
  int charsetIndex();

  /**
   * 设置响应结束,即payload写入结束
   */
  void setResponseFinished(boolean b);
  /**
   * 可能用于实现 reset connection命令
   */
  void resetSession();

  /**
   * 连接相关字符集
   */
  Charset charset();

  /**
   * 写入文本结果集行
   */
  default void writeTextRowPacket(byte[][] row) {
    byte[] bytes = MySQLPacketUtil.generateTextRow(row);
    writeBytes(bytes,false);
  }
  /**
   * 写入二进制结果集行
   */
  default void writeBinaryRowPacket(byte[][] row) {
    byte[] bytes = MySQLPacketUtil.generateBinaryRow(row);
    writeBytes(bytes,false);
  }

  /**
   * 写入字段数
   */
  default void writeColumnCount(int count) {
    byte[] bytes = MySQLPacketUtil.generateResultSetCount(count);
    writeBytes(bytes,false);
  }


  /**
   * 写入字段
   */
  default void writeColumnDef(String columnName, int type) {
    byte[] bytes = MySQLPacketUtil
        .generateColumnDefPayload(columnName, type, charsetIndex(), charset());
    writeBytes(bytes,false);
  }

  void writeBytes(byte[] payload,boolean end);

  /**
   * 写入ok包,调用该方法,就指定响应已经结束
   */
  default void writeOkEndPacket() {
    byte[] bytes = MySQLPacketUtil
        .generateOk(0, getWarningCount(), getServerStatus(), affectedRows(),
            getLastInsertId(),
            MySQLServerCapabilityFlags.isClientProtocol41(getCapabilities()),
            MySQLServerCapabilityFlags.isKnowsAboutTransactions(getCapabilities()),
            false, ""

        );
    writeBytes(bytes,true);
  }

  /**
   * 写入字段阶段技术报文,即字段包都写入后调用此方法
   */
  default void writeColumnEndPacket() {
    if (isDeprecateEOF()) {
    } else {
      byte[] bytes = MySQLPacketUtil.generateEof(getWarningCount(), getServerStatus());
      writeBytes(bytes,true);
    }
  }

  /**
   * 结果集结束写入该报文,需要指定是否有后续的结果集和是否有游标
   */
  default void writeRowEndPacket(boolean hasMoreResult, boolean hasCursor) {
    byte[] bytes;
    int serverStatus = getServerStatus();
    if (hasMoreResult) {
      serverStatus |= MySQLServerStatusFlags.MORE_RESULTS;
    }
    if (hasCursor) {
      serverStatus |= MySQLServerStatusFlags.CURSOR_EXISTS;
    }
    if (isDeprecateEOF()) {
      bytes = MySQLPacketUtil
          .generateOk(0xfe, getWarningCount(), serverStatus, affectedRows(),
              getLastInsertId(),
              MySQLServerCapabilityFlags.isClientProtocol41(getCapabilities()),
              MySQLServerCapabilityFlags.isKnowsAboutTransactions(getCapabilities()),
              MySQLServerCapabilityFlags.isSessionVariableTracking(getCapabilities()),
              getLastMessage());
    } else {
      bytes = MySQLPacketUtil.generateEof(getWarningCount(), getServerStatus());
    }
    writeBytes(bytes,true);
  }

  /**
   * 根据session的信息写入错误包,所以错误包的信息要设置session
   */
  default void writeErrorEndPacket() {
    int lastErrorCode = getLastErrorCode();
    if (lastErrorCode == 0) {
      lastErrorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
    }
    byte[] bytes = MySQLPacketUtil
        .generateError(lastErrorCode, getLastMessage(), this.getServerStatus());
    writeBytes(bytes,true);
  }
  default void writeErrorEndPacket(ErrorPacketImpl packet) {
    int lastErrorCode = packet.getErrorCode();
    if (lastErrorCode == 0) {
      lastErrorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
    }
    try(MySQLPayloadWriter writer = new MySQLPayloadWriter()){
      packet.writePayload(writer,getCapabilities());
      writeBytes(writer.toByteArray(),true);
    }
  }

  void writeErrorEndPacketBySyncInProcessError();

  void writeErrorEndPacketBySyncInProcessError(int errorCode);

  void writeErrorEndPacketBySyncInProcessError(int packetId, int errorCode);

}
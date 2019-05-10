package io.mycat.proxy.session;

import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.MycatSessionWriteHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.PacketSplitter;
import io.mycat.proxy.payload.MySQLPacketUtil;
import io.mycat.util.StringUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.LinkedList;

/**
 * @author jamie12221
 * @date 2019-05-08 00:06
 **/
public interface MySQLServerSession<T extends Session<T>> extends Session<T> {

  LinkedList<ByteBuffer> writeQueue();

  BufferPool bufferPool();

  ByteBuffer packetHeaderBuffer();

  ByteBuffer[] packetContainer();

  void setPakcetId(int packet);

  byte getNextPacketId();

  PacketSplitter packetSplitter();

  String lastMessage();

  long affectedRows();

  long incrementAffectedRows();

  int serverStatus();

  int setServerStatus(int s);

  int incrementWarningCount();

  int warningCount();

  long lastInsertId();

  int setLastInsertId(int s);

  int lastErrorCode();

  boolean isDeprecateEOF();

  void resetSession();

  Charset charset();

  int charsetIndex();

  int capabilities();

  void setLastErrorCode(int errorCode);

  boolean isResponseFinished();

  void setResponseFinished(boolean b);

  final static byte[] SQL_STATE = "HY000".getBytes();

  public default void writeTextRowPacket(byte[][] row) throws IOException {
    byte[] bytes = MySQLPacketUtil.generateTextRow(row);
    writeBytes(bytes);
  }

  public default void writeBinaryRowPacket(byte[][] row) throws IOException {
    byte[] bytes = MySQLPacketUtil.generateBinaryRow(row);
    writeBytes(bytes);
  }

  public default void writeColumnCount(int count) throws IOException {
    byte[] bytes = MySQLPacketUtil.generateResultSetCount(count);
    writeBytes(bytes);
  }

  public default void writeColumnDef(String columnName, int type) throws IOException {
    byte[] bytes = MySQLPacketUtil
                       .generateColumnDef(columnName, type, charsetIndex(), charset());
    writeBytes(bytes);
  }

  public default void writeOkEndPacket() throws IOException {
    this.setResponseFinished(true);
    byte[] bytes = MySQLPacketUtil
                       .generateOk(0, warningCount(), serverStatus(), affectedRows(),
                           lastInsertId(),
                           MySQLServerCapabilityFlags.isClientProtocol41(capabilities()),
                           MySQLServerCapabilityFlags.isKnowsAboutTransactions(capabilities()),
                           false, ""

                       );
    writeBytes(bytes);
  }

  default void writeBytes(byte[] bytes) {
    try {
      ByteBuffer buffer = bufferPool().allocate(bytes);
      writeQueue().push(buffer);
      writeToChannel();
    }catch (Exception e){
      this.close(false,e.getMessage());
    }
  }

  public default void writeColumnEndPacket() throws IOException {
    if (isDeprecateEOF()) {
    } else {
      byte[] bytes = MySQLPacketUtil.generateEof(warningCount(), serverStatus());
      writeBytes(bytes);
    }
  }

  public default void writeRowEndPacket(boolean hasMoreResult) throws IOException {
    this.setResponseFinished(true);
    byte[] bytes;
    if (isDeprecateEOF()) {
      bytes = MySQLPacketUtil.generateOk(0xfe, warningCount(), 2, affectedRows(),
          lastInsertId(), MySQLServerCapabilityFlags.isClientProtocol41(capabilities()),
          MySQLServerCapabilityFlags.isKnowsAboutTransactions(capabilities()),
          MySQLServerCapabilityFlags.isSessionVariableTracking(capabilities()),
          lastMessage());
    } else {
      bytes = MySQLPacketUtil.generateEof(warningCount(), serverStatus());
    }
    writeBytes(bytes);
  }
  public default void writeErrorEndPacket(){
    this.setResponseFinished(true);
    byte[] bytes = MySQLPacketUtil
                       .generateError(lastErrorCode(), lastMessage(), this.capabilities());
    writeBytes(bytes);
  }


  public default void writeToChannel() throws IOException {
    writeToChannel(this);
  }

  public static enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession session) throws IOException {
      MySQLServerSession.writeToChannel(session);
    }

    @Override
    public void onWriteFinished(MycatSession session) throws IOException {

    }
  }


  public static void writeToChannel(MySQLServerSession session) throws IOException {
    LinkedList<ByteBuffer> byteBuffers = session.writeQueue();
    ByteBuffer[] packetContainer = session.packetContainer();
    PacketSplitter packetSplitter = session.packetSplitter();
    long writed;
    do {
      writed = 0;
      if (byteBuffers.isEmpty()) {
        break;
      }
      ByteBuffer first = byteBuffers.peekFirst();

      if (first.position() == 0) {//一个全新的payload
        packetSplitter.init(first.limit());
        packetSplitter.nextPacketInPacketSplitter();
        splitPacket(session, packetContainer, packetSplitter, first);
        writed = session.channel().write(packetContainer);
        if (first.hasRemaining()) {
          return;
        } else {
          continue;
        }
      } else {
        writed = session.channel().write(packetContainer);
        if (first.hasRemaining()) {
          return;
        } else {
          if (packetSplitter.nextPacketInPacketSplitter()) {
            splitPacket(session, packetContainer, packetSplitter, first);
            writed = session.channel().write(packetContainer);
            if (first.hasRemaining()) {
              return;
            } else {
              continue;
            }
          } else {
            System.out.println(StringUtil.dumpAsHex(packetContainer[0]));
            System.out.println(StringUtil.dumpAsHex(packetContainer[1]));
            byteBuffers.removeFirst();
            session.bufferPool().recycle(first);
          }
        }
      }
    } while (writed > 0);
    if (writed == -1) {
      logger.warn("Write EOF ,socket closed ");
      throw new ClosedChannelException();
    }
    if (byteBuffers.isEmpty() && session.isResponseFinished()) {
      session.writeFinished(session);
      return;
    }

  }

  static void splitPacket(MySQLServerSession session, ByteBuffer[] packetContainer,
      PacketSplitter packetSplitter,
      ByteBuffer first) {
    int offset = packetSplitter.getOffsetInPacketSplitter();
    int len = packetSplitter.getPacketLenInPacketSplitter();
    setPacketHeader(session, packetContainer, len);

    first.position(offset).limit(len + offset);
    packetContainer[1] = first;
  }

  static void setPacketHeader(MySQLServerSession session, ByteBuffer[] packetContainer, int len) {
    ByteBuffer header = session.packetHeaderBuffer();
    header.position(0).limit(4);
    MySQLPacket.writeFixIntByteBuffer(header, 3, len);
    byte nextPacketId = session.getNextPacketId();
    header.put(nextPacketId);
    packetContainer[0] = header;
    header.flip();
  }


}

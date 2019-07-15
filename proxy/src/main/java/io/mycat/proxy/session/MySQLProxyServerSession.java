package io.mycat.proxy.session;

import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.buffer.CrossSwapThreadBufferPool;
import io.mycat.proxy.handler.MycatHandler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Queue;

/**
 * @author jamie12221 date 2019-05-08 00:06
 *
 * mysql server session 该接口实现服务器模式
 **/
public interface MySQLProxyServerSession<T extends Session<T>> extends MySQLServerSession<T> {


  CrossSwapThreadBufferPool writeBufferPool();

  /**
   * 前端写入队列
   */
  Queue<ByteBuffer> writeQueue();

  /**
   * mysql 报文头 辅助buffer
   */
  ByteBuffer packetHeaderBuffer();

  /**
   * mysql 报文辅助buffer
   */
  ByteBuffer[] packetContainer();


  /**
   * 前端写入处理器可能有多种,此为设置服务器模式
   */
  void switchMySQLServerWriteHandler();

  MycatReactorThread getIOThread();


  /**
   * 写入payload
   */
  default void writeBytes(byte[] payload, boolean end) {
    try {
      switchMySQLServerWriteHandler();
      ByteBuffer buffer = writeBufferPool().allocate(payload);
      boolean ioThread = Thread.currentThread() == getIOThread();
      if (!ioThread && end) {
        backFromWorkerThread();
      }
      setResponseFinished(end);
      Queue<ByteBuffer> byteBuffers = writeQueue();
      while (!byteBuffers.offer(buffer)) { }
      if (end){
        while (!byteBuffers.offer(END_PACKET)) { }
      }
      if (ioThread) {
        writeToChannel();
      } else {
        this.change2WriteOpts();
        if (end) {
          getIOThread().getSelector().wakeup();
        }
      }
    } catch (Exception e) {
      this.close(false, setLastMessage(e));
    }
  }

  void backFromWorkerThread();


  default void writeToChannel() throws IOException {
    writeToChannel(this);
  }

  default void writeErrorEndPacketBySyncInProcessError() {
    writeErrorEndPacketBySyncInProcessError(MySQLErrorCode.ER_UNKNOWN_ERROR);
  }

  default void writeErrorEndPacketBySyncInProcessError(int errorCode) {
    writeErrorEndPacketBySyncInProcessError(1, errorCode);
  }

  /**
   * 同步写入错误包,用于异常处理,一般错误包比较小,一次非阻塞写入就结束了,写入不完整尝试四次, 之后就会把mycat session关闭,简化错误处理
   */
  default void writeErrorEndPacketBySyncInProcessError(int packetId, int errorCode) {
    setLastErrorCode(errorCode);
    switchMySQLServerWriteHandler();
    this.setResponseFinished(true);
    byte[] bytes = MySQLPacketUtil
        .generateError(errorCode, getLastMessage(),
            this.getCapabilities());
    byte[] bytes1 = MySQLPacketUtil.generateMySQLPacket(packetId, bytes);
    ByteBuffer message = ByteBuffer.wrap(bytes1);
    int counter = 0;
    try {
      SocketChannel channel = channel();
      if (channel.isOpen()) {
        while (message.hasRemaining() && counter < 4) {
          channel().write(message);
          counter++;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  MySQLPacketSplitter packetSplitter();

  /**
   * 前端写入处理器
   */
  enum WriteHandler implements MycatSessionWriteHandler {
    INSTANCE;

    @Override
    public void writeToChannel(MycatSession session) throws IOException {
      MySQLProxyServerSession.writeToChannel(session);
    }

    @Override
    public void onException(MycatSession session, Exception e) {
      MycatMonitor.onMycatServerWriteException(session, e);
      session.resetPacket();
    }

  }

  ByteBuffer END_PACKET = ByteBuffer.allocate(0);


  /**
   * 该函数实现Payload到packet的转化 所以队列里面都是Payload
   *
   * 把队列的buffer写入通道,一个buffer是一个payload,写入时候转化成packet 写入的 clearReadWriteOpts byteBuffers
   * isResponseFinished
   *
   * 与另外一个线程的 io.mycat.proxy.session.MySQLProxyServerSession#writeBytes(byte[])
   *
   * change2WriteOpts byteBuffers isResponseFinished设置
   *
   * 应该互斥
   */
  static void writeToChannel(MySQLProxyServerSession session) throws IOException {
    Queue<ByteBuffer> byteBuffers = session.writeQueue();
    ByteBuffer[] packetContainer = session.packetContainer();
    MySQLPacketSplitter packetSplitter = session.packetSplitter();
    long writed;
    do {
      writed = 0;
      if (byteBuffers.isEmpty()) {
        break;
      }
      ByteBuffer first = byteBuffers.peek();

      if (END_PACKET == first) {
        break;
      }

      if (first.position() == 0) {//一个全新的payload
        MycatMonitor.onFrontWrite(
            session, first, 0, first.limit());
        packetSplitter.init(first.limit());
        packetSplitter.nextPacketInPacketSplitter();
        splitPacket(session, packetContainer, packetSplitter, first);
        assert packetContainer[0] != null;
        assert packetContainer[1] != null;
        writed = session.channel().write(packetContainer);
        if (first.hasRemaining()) {
          return;
        } else {
          continue;
        }
      } else {
        assert packetContainer[0] != null;
        assert packetContainer[1] != null;
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
            byteBuffers.remove();
            session.writeBufferPool().recycle(first);
          }
        }
      }
    } while (writed > 0);
    if (writed == -1) {
      throw new ClosedChannelException();
    }
    boolean writeFinished = byteBuffers.peek() == END_PACKET;
    if (!writeFinished) {
      if (writed == 0 || !byteBuffers.isEmpty()) {
        session.change2WriteOpts();
      } else {
        session.clearReadWriteOpts();
        if (!byteBuffers.isEmpty()) {
          writeToChannel(session);
          return;
        }
      }
    } else {
      byteBuffers.clear();
      session.writeFinished(session);
      return;
    }
  }

  /**
   * 生成packet
   */
  static void splitPacket(MySQLProxyServerSession session, ByteBuffer[] packetContainer,
      MySQLPacketSplitter packetSplitter,
      ByteBuffer first) {
    int offset = packetSplitter.getOffsetInPacketSplitter();
    int len = packetSplitter.getPacketLenInPacketSplitter();
    setPacketHeader(session, packetContainer, len);

    first.position(offset).limit(len + offset);
    packetContainer[1] = first;
  }

  /**
   * 构造packet header
   */
  static void setPacketHeader(MySQLProxyServerSession session, ByteBuffer[] packetContainer,
      int len) {
    ByteBuffer header = session.packetHeaderBuffer();
    header.position(0).limit(4);
    MySQLPacket.writeFixIntByteBuffer(header, 3, len);
    byte nextPacketId = session.getNextPacketId();
    header.put(nextPacketId);
    packetContainer[0] = header;
    header.flip();
  }
}

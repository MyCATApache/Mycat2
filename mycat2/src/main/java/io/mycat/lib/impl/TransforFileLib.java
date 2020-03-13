package io.mycat.lib.impl;

import io.mycat.MycatException;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.handler.MycatSessionWriteHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ProcessState;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * @author chen junwen
 */
public class TransforFileLib {
    public static Response transferFileTo(String file) {
        return new Response() {
            @Override
            public void apply(MycatSession session) {
                TransforFileLib.WriteHandler writeHandler = new TransforFileLib.WriteHandler(session, file);
                session.switchWriteHandler(writeHandler);
                try {
                    session.writeToChannel();
                } catch (IOException e) {
                    session.setLastMessage(e);
                    session.writeErrorEndPacketBySyncInProcessError();
                    writeHandler.onException(session,e);
                    session.getCurNIOHandler().onException(session,e);
                }
            }
        };
    }
    public static void saveToFile(String filePath,boolean eof,MycatResultSetResponse<byte[]> resultSetResponse) throws IOException {
        int columnCount = resultSetResponse.columnCount();
        byte packetId = 1;
        byte[] bytes = MySQLPacketUtil.generateMySQLPacket(packetId++, MySQLPacketUtil.generateResultSetCount(columnCount));
        RandomAccessFile file = new RandomAccessFile(filePath,"rw");
        file.write(bytes);
        Iterator columnDefIterator = resultSetResponse.columnDefIterator();
        while (columnDefIterator.hasNext()){
            byte[] next = (byte[]) columnDefIterator.next();
            file.write( MySQLPacketUtil.generateMySQLPacket(packetId++,next));
        }
        if (eof){
            file.write( MySQLPacketUtil.generateMySQLPacket(packetId++,MySQLPacketUtil.generateEof(0,0)));
        }
        Iterator rowIterator = resultSetResponse.rowIterator();
        while (rowIterator.hasNext()){
            byte[] next = (byte[]) rowIterator.next();
            file.write( MySQLPacketUtil.generateMySQLPacket(packetId++,next));
        }
        file.write( MySQLPacketUtil.generateMySQLPacket(packetId++,MySQLPacketUtil.generateEof(0,0)));
        file.close();
    }
    /**
     * 前端写入处理器
     */
    public static class WriteHandler implements MycatSessionWriteHandler {
        final MycatSession session;
        final String file;
        final FileChannel fileChannel;

        long position;
        private long length;

        public WriteHandler(MycatSession session, String file) {
            this(session, file, 0, -1);
        }

        public WriteHandler(MycatSession session, String file, long position, long length) {
            this.session = session;
            this.file = file;
            this.position = position;
            this.length = length;
            try {
                Path path = Paths.get(file).toAbsolutePath();
                fileChannel = FileChannel.open(path);
                if (this.length == -1) {
                    this.length = fileChannel.size();
                }
            } catch (Exception e) {
                throw new MycatException(e);
            }
        }

        @Override
        public void writeToChannel(MycatSession session) throws IOException {
            session.setResponseFinished(ProcessState.DOING);
            long writed = fileChannel.transferTo(this.position, this.length, session.channel());
            this.position += writed;
            this.length -= writed;
            if (this.length == 0) {
                fileChannel.close();
                session.setResponseFinished(ProcessState.DONE);
                session.writeFinished(session);
            } else {
                session.change2WriteOpts();
            }
        }

        @Override
        public void onException(MycatSession session, Exception e) {
            if (fileChannel!=null){
                try {
                    fileChannel.close();
                } catch (IOException e1) {

                }
            }
            MycatMonitor.onMycatServerWriteException(session, e);
            session.resetPacket();
        }

        @Override
        public void onLastPacket(MycatSession session) {

        }

        @Override
        public WriteType getType() {
            return WriteType.PROXY;
        }

    }
}
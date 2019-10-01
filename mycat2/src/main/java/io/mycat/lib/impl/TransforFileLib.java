package io.mycat.lib.impl;

import cn.lightfish.pattern.DynamicSQLMatcher;
import io.mycat.MycatException;
import io.mycat.proxy.handler.MycatHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ProcessState;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TransforFileLib {
    public static Response transferTo(String file) {
        return new Response() {
            @Override
            public void apply(MycatSession session, DynamicSQLMatcher matcher) {
                TransforFileLib.WriteHandler writeHandler = new TransforFileLib.WriteHandler(session, file);
                session.switchWriteHandler(writeHandler);
                try {
                    session.writeToChannel();
                } catch (IOException e) {
                    session.setLastMessage(e);
                    session.writeErrorEndPacketBySyncInProcessError();
                    session.getCurNIOHandler().onException(session,e);
                }
            }
        };
    }
    /**
     * 前端写入处理器
     */
    public static class WriteHandler implements MycatHandler.MycatSessionWriteHandler {
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
            MycatMonitor.onMycatServerWriteException(session, e);
            session.resetPacket();
        }

    }
}
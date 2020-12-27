/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.x;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.xdevapi.*;
import io.mycat.NameableThreadFactory;
import lombok.Setter;

/**
 * Synchronous-only implementation of {@link MessageReader}. This implementation wraps a {@link java.io.InputStream}.
 */
public class SyncMessageReader implements MessageReader<XMessageHeader, XMessage> {
    @Setter
    private static Executor executor = Executors.newFixedThreadPool(
            1,new NameableThreadFactory("mysql-protocolx-poll-",false));

    /** Stream as a source of messages. */
    private FullReadInputStream inputStream;

    private XMessageHeader header;

    /** Queue of <code>MessageListener</code>s waiting to process messages. */
    BlockingQueue<MessageListener<XMessage>> messageListenerQueue = new LinkedBlockingQueue<>();

    /** Lock to protect the pending message. */
    Object dispatchingThreadMonitor = new Object();
    /** Lock to protect async reads from sync ones. */
    Object waitingSyncOperationMonitor = new Object();

    Thread dispatchingThread = null;

    public SyncMessageReader(FullReadInputStream inputStream) {
        this.inputStream = inputStream;
    }

    public static void main(String[] args) {
        ClientFactory clientFactory = new ClientFactory();
        Client client = clientFactory.getClient("mysqlx://localhost:3306/local?user=root&password=123456", "{\"pooling\":{\"enabled\":true, \"maxSize\":8,\"maxIdleTime\":30000, \"queueTimeout\":10000} }");
        Session session = client.getSession();
        SqlStatement sql = session.sql("select * from information_schema.USER_PRIVILEGES");

        Executor mycallback = Executors.newCachedThreadPool();

        CompletableFuture<SqlResult> completableFuture = sql.executeAsync();
        completableFuture.whenCompleteAsync((sqlResult,th)->{
            System.out.println("sqlResult = " + sqlResult);
        },mycallback);
        client.close();
    }

    @Override
    public XMessageHeader readHeader() throws IOException {
        // waiting for ListenersDispatcher completion to perform sync call
        synchronized (this.waitingSyncOperationMonitor) {
            if (this.header == null) {
                this.header = readHeaderLocal();
            }
            if (this.header.getMessageType() == ServerMessages.Type.ERROR_VALUE) {
                throw new XProtocolError(readMessageLocal(Error.class));
            }
            return this.header;
        }
    }

    private XMessageHeader readHeaderLocal() throws IOException {

        try {
            /*
             * Note that the "header" per-se is the size of all data following the header. This currently includes the message type tag (1 byte) and the
             * message bytes. However since we know the type tag is present we also read it as part of the header. This may change in the future if session
             * multiplexing is supported by the protocol. The protocol will be able to accommodate it but we will have to separate reading data after the
             * header (size).
             */
            byte[] len = new byte[5];
            this.inputStream.readFully(len);
            this.header = new XMessageHeader(len);
        } catch (IOException ex) {
            // TODO close socket?
            throw new CJCommunicationsException("Cannot read packet header", ex);
        }

        return this.header;
    }

    @SuppressWarnings("unchecked")
    private <T extends GeneratedMessageV3> T readMessageLocal(Class<T> messageClass) {
        Parser<T> parser = (Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
        byte[] packet = new byte[this.header.getMessageSize()];

        try {
            this.inputStream.readFully(packet);
        } catch (IOException ex) {
            // TODO close socket?
            throw new CJCommunicationsException("Cannot read packet payload", ex);
        }

        try {
            return parser.parseFrom(packet);
        } catch (InvalidProtocolBufferException ex) {
            throw new WrongArgumentException(ex);
        } finally {
            // This must happen if we *successfully* read a packet. CJCommunicationsException will be thrown above if not
            this.header = null;
        }
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, XMessageHeader hdr) throws IOException {
        return readMessage(reuse, hdr.getMessageType());
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, int expectedType) throws IOException {
        // waiting for ListenersDispatcher completion to perform sync call
        synchronized (this.waitingSyncOperationMonitor) {
            try {
                Class<? extends GeneratedMessageV3> expectedClass = MessageConstants.getMessageClassForType(expectedType);

                List<Notice> notices = null;
                XMessageHeader hdr;
                while ((hdr = readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE && expectedType != ServerMessages.Type.NOTICE_VALUE) {
                    if (notices == null) {
                        notices = new ArrayList<>();
                    }
                    notices.add(Notice.getInstance(new XMessage(readMessageLocal(MessageConstants.getMessageClassForType(ServerMessages.Type.NOTICE_VALUE)))));
                }

                Class<? extends GeneratedMessageV3> messageClass = MessageConstants.getMessageClassForType(hdr.getMessageType());
                // ensure that parsed message class matches incoming tag
                if (expectedClass != messageClass) {
                    throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '"
                            + messageClass.getSimpleName() + "'");
                }

                return new XMessage(readMessageLocal(messageClass)).addNotices(notices);
            } catch (IOException e) {
                throw new XProtocolError(e.getMessage(), e);
            }
        }
    }

    public void pushMessageListener(final MessageListener<XMessage> listener) {
        try {
            this.messageListenerQueue.put(listener);
        } catch (InterruptedException e) {
            throw new CJCommunicationsException("Cannot queue message listener.", e);
        }

        synchronized (this.dispatchingThreadMonitor) {
            if (this.dispatchingThread == null) {
                ListenersDispatcher ld = new ListenersDispatcher();
//                this.dispatchingThread = new Thread(ld, "Message listeners dispatching thread");
//                this.dispatchingThread.start();

                executor.execute(ld);

                // We must ensure that ListenersDispatcher is really started before leaving
                // the synchronized block. Otherwise the race condition is possible: if next
                // operation is executed synchronously it could consume results of the previous
                // asynchronous operation.
                int millis = 5000; // TODO expose via properties ?
                while (!ld.started) {
                    try {
                        Thread.sleep(10);
                        millis = millis - 10;
                    } catch (InterruptedException e) {
                        throw new XProtocolError(e.getMessage(), e);
                    }
                    if (millis <= 0) {
                        throw new XProtocolError("Timeout for starting ListenersDispatcher exceeded.");
                    }
                }
            }
        }
    }

    private class ListenersDispatcher implements Runnable {
        /**
         * The timeout value for queue.poll(timeout, unit) defining the time after which we close and unregister the dispatching thread.
         * On the other hand, a bigger timeout value allows us to keep dispatcher thread running while multiple concurrent asynchronous
         * read operations are pending, thus avoiding the delays for new dispatching threads creation.
         */
        private static final long POLL_TIMEOUT = 100; // TODO expose via connection property
        boolean started = false;

        public ListenersDispatcher() {
        }

        @Override
        public void run() {
            synchronized (SyncMessageReader.this.waitingSyncOperationMonitor) {
                this.started = true;
                try {
                    while (true) {
                        MessageListener<XMessage> l;
                        if ((l = SyncMessageReader.this.messageListenerQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS)) == null) {
                            synchronized (SyncMessageReader.this.dispatchingThreadMonitor) {
                                if (SyncMessageReader.this.messageListenerQueue.peek() == null) {
                                    SyncMessageReader.this.dispatchingThread = null;
                                    break;
                                }
                            }
                        } else {
                            try {
                                XMessage msg = null;
                                do {
                                    XMessageHeader hdr = readHeader();
                                    msg = readMessage(null, hdr);
                                } while (!l.processMessage(msg));
                            } catch (Throwable t) {
                                l.error(t);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new CJCommunicationsException("Read operation interrupted.", e);
                }
            }
        }
    }
}

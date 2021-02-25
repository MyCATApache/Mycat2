package io.mycat.vertxmycat;

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.session.MySQLClientSession;
import io.vertx.core.Promise;

public class CommandResponse implements ResultSetCallBack<MySQLClientSession> {
        private final Promise<Void> promise;

        public CommandResponse(Promise<Void> promise) {
            this.promise = promise;
        }

        @Override
        public void onFinishedSendException(Exception exception, Object sender,
                                            Object attr) {
            promise.tryFail(exception);
        }

        @Override
        public void onFinishedException(Exception exception, Object sender, Object attr) {
            promise.tryFail(exception);
        }

        @Override
        public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                               Object attr) {
            promise.tryComplete();
        }

        @Override
        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                  MySQLClientSession mysql, Object sender, Object attr) {
            promise.tryFail(new MycatException(errorPacket.getErrorCode(), errorPacket.getErrorMessageString()));
        }
    }

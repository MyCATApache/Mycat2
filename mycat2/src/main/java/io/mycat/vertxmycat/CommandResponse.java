/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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

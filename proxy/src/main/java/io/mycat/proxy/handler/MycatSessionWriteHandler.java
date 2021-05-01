/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.proxy.handler;

import io.mycat.proxy.session.MycatSession;
import io.vertx.core.impl.future.PromiseInternal;

import java.io.IOException;

/**
 * mycat session写入处理
 */
public interface MycatSessionWriteHandler {

    /**
     * Write to channel.
     *
     * @param session the session
     * @throws IOException the io exception
     */
    PromiseInternal<Void> writeToChannel(MycatSession session) throws IOException;

    /**
     * On exception.
     *
     * @param session the session
     * @param e       the e
     */
    void onException(MycatSession session, Exception e);

    void onClear(MycatSession session);

    WriteType getType();

    public enum WriteType {
        SERVER,
        PROXY
    }
}
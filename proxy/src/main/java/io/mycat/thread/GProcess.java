/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.thread;

import io.mycat.bindthread.BindThreadCallback;
import io.mycat.bindthread.BindThreadKey;
import io.mycat.proxy.session.Session;

/**
 * @author Junwen Chen
 **/
public abstract class GProcess<T extends BindThreadKey & Session> implements BindThreadCallback<T, GThread> {

    @Override
    public void accept(T key, GThread context) {
        context.setCurSession(key);
        accept(key);
    }

    public abstract void accept(T key);
}
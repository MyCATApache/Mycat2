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
package io.mycat.proxy.reactor;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class ReactorThreadManager {
    final CopyOnWriteArrayList<MycatReactorThread> list;


    public List<MycatReactorThread> getList() {
        return Collections.unmodifiableList(list);
    }

    public ReactorThreadManager(List<MycatReactorThread> list) {
        this.list = new CopyOnWriteArrayList<>(list);
    }

    public synchronized MycatReactorThread getRandomReactor() {
        return list.get(ThreadLocalRandom.current().nextInt(0, list.size() ));
    }

    public synchronized void add(MycatReactorThread thread) {
        list.add(thread);
    }
    public synchronized void remove(MycatReactorThread thread) {
        list.remove(thread);
    }

}
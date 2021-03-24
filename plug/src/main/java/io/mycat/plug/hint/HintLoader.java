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
package io.mycat.plug.hint;

import io.mycat.Hint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum HintLoader {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(HintLoader.class);
    final ConcurrentMap<String, Hint> map = new ConcurrentHashMap<>();

    public Hint get(String name) {
        return map.get(name);
    }

    public Hint getOrDefault(String name, Hint defaultHint) {
        return map.getOrDefault(name, defaultHint);
    }

    public void register(String name, Hint hint) {
        map.put(name, hint);
    }

}
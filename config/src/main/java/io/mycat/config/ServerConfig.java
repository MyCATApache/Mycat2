/**
 * Copyright (C) <2019>  <gaozhiwen>
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

package io.mycat.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Data
public class ServerConfig {
    private String ip = "0.0.0.0";
    private int port = 8066;
    private int reactorNumber = 1;
    private String handlerName;
    private Worker worker = new Worker();
    private BufferPoolConfig bufferPool= new BufferPoolConfig();

    @Data
    public static class Worker {
        private int minThread = 2;
        private int maxThread = 2;
        private int waitTaskTimeout = 5;
        private String timeUnit = TimeUnit.SECONDS.toString();
        private int maxPengdingLimit = 65535;
        private boolean close = false;
    }
    @Data
    public static class BufferPoolConfig {
        String poolName;
        Map<String,String> args = new HashMap<>();
    }
}

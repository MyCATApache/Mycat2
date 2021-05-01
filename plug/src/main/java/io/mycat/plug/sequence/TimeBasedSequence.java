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
package io.mycat.plug.sequence;

import com.imadcn.framework.idworker.algorithm.Snowflake;
import io.mycat.config.SequenceConfig;

public class TimeBasedSequence implements SequenceHandler {

    private long workerId;
    private Snowflake snowflake;

    @Override
    public void init(SequenceConfig config, long workerId) {
        this.workerId = workerId;
        this.snowflake = Snowflake.create(workerId);
    }

    @Override
    public void setStart(Number value) {

    }


    public Number nextId() {
        return snowflake.nextId();
    }

    @Override
    public Number get() {
        return nextId();
    }
}

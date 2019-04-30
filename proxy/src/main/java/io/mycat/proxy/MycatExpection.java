/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy;

import io.mycat.proxy.packet.ErrorCode;

public class MycatExpection extends RuntimeException {

    private final int errorCode;

    public int getErrorCode() {
        return errorCode;
    }
    public MycatExpection(String message) {
        super(message);
        this.errorCode = ErrorCode.ER_UNKNOWN_ERROR;
    }
    public MycatExpection(int errorCode,String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MycatExpection(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.ER_UNKNOWN_ERROR;
    }
}

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
package io.mycat;


import io.mycat.beans.mysql.MySQLErrorCode;
import org.slf4j.helpers.MessageFormatter;

/**
 * @author jamie12221
 *  date 2019-05-05 23:30 mycat 异常
 **/
public class MycatException extends RuntimeException {

    private int errorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;

    public MycatException(String message) {
        super(message);
        this.errorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
    }

    public MycatException(Exception message) {
        super(message);
        this.errorCode = MySQLErrorCode.ER_UNKNOWN_ERROR;
    }

    public MycatException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MycatException(String message, Object... args) {
        super(MessageFormatter.arrayFormat(message, args).getMessage());
    }

    public MycatException(int errorCode, String message, Throwable throwable) {
        super(message, throwable);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}

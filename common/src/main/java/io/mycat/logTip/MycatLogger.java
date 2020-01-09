/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.logTip;

import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import static io.mycat.beans.mysql.MySQLErrorCode.ER_UNKNOWN_ERROR;

public class MycatLogger {

  final Logger logger;

  MycatLogger(Class<?> clazz) {
    logger = LoggerFactory.getLogger(clazz);
  }

  MycatLogger(String clazz) {
    logger = LoggerFactory.getLogger(clazz);
  }

  public String getName() {
    return logger.getName();
  }

  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  public void trace(String s) {
    logger.trace(s);
  }

  public void trace(String s, Object o) {
    logger.trace(s, o);
  }

  public void trace(String s, Object o, Object o1) {
    logger.trace(s, o, o1);
  }

  public void trace(String s, Object... objects) {
    logger.trace(s, objects);
  }

  public void trace(String s, Throwable throwable) {
    logger.trace(s, throwable);
  }

  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled(marker);
  }

  public void trace(Marker marker, String s) {
    logger.trace(marker, s);
  }

  public void trace(Marker marker, String s, Object o) {
    logger.trace(marker, s, o);
  }

  public void trace(Marker marker, String s, Object o, Object o1) {
    logger.trace(marker, s, o, o1);
  }

  public void trace(Marker marker, String s, Object... objects) {
    logger.trace(marker, s, objects);
  }

  public void trace(Marker marker, String s, Throwable throwable) {
    logger.trace(marker, s, throwable);
  }

  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  public void debug(String s) {
    logger.debug(s);
  }

  public void debug(String s, Object o) {
    logger.debug(s, o);
  }

  public void debug(String s, Object o, Object o1) {
    logger.debug(s, o, o1);
  }

  public void debug(String s, Object... objects) {
    logger.debug(s, objects);
  }

  public void debug(String s, Throwable throwable) {
    logger.debug(s, throwable);
  }

  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  public void debug(Marker marker, String s) {
    logger.debug(marker, s);
  }

  public void debug(Marker marker, String s, Object o) {
    logger.debug(marker, s, o);
  }

  public void debug(Marker marker, String s, Object o, Object o1) {
    logger.debug(marker, s, o, o1);
  }

  public void debug(Marker marker, String s, Object... objects) {
    logger.debug(marker, s, objects);
  }

  public void debug(Marker marker, String s, Throwable throwable) {
    logger.debug(marker, s, throwable);
  }

  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  public void info(String s) {
    logger.info(s);
  }

  public void info(String s, Object o) {
    logger.info(s, o);
  }

  public void info(String s, Object o, Object o1) {
    logger.info(s, o, o1);
  }

  public void info(String s, Object... objects) {
    logger.info(s, objects);
  }

  public void info(String s, Throwable throwable) {
    logger.info(s, throwable);
  }

  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  public void info(Marker marker, String s) {
    logger.info(marker, s);
  }

  public void info(Marker marker, String s, Object o) {
    logger.info(marker, s, o);
  }

  public void info(Marker marker, String s, Object o, Object o1) {
    logger.info(marker, s, o, o1);
  }

  public void info(Marker marker, String s, Object... objects) {
    logger.info(marker, s, objects);
  }

  public void info(Marker marker, String s, Throwable throwable) {
    logger.info(marker, s, throwable);
  }

  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  public void warn(String s) {
    logger.warn(s);
  }

  public void warn(String s, Object o) {
    logger.warn(s, o);
  }

  public void warn(String s, Object... objects) {
    logger.warn(s, objects);
  }

  public void warn(String s, Object o, Object o1) {
    logger.warn(s, o, o1);
  }

  public void warn(String s, Throwable throwable) {
    logger.warn(s, throwable);
  }

  public boolean isWarnEnabled(Marker marker) {
    return logger.isWarnEnabled(marker);
  }

  public void warn(Marker marker, String s) {
    logger.warn(marker, s);
  }

  public void warn(Marker marker, String s, Object o) {
    logger.warn(marker, s, o);
  }

  public void warn(Marker marker, String s, Object o, Object o1) {
    logger.warn(marker, s, o, o1);
  }

  public void warn(Marker marker, String s, Object... objects) {
    logger.warn(marker, s, objects);
  }

  public void warn(Marker marker, String s, Throwable throwable) {
    logger.warn(marker, s, throwable);
  }

  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  public void error(String message) {
    logger.error(message);
  }

  public void error(String message, Object o, Object o2) {
    logger.error(message, o, o2);
  }

  public void error(String message, Object... os) {
    logger.error(message, os);
  }

  public void error(String message, Throwable e) {
    logger.error(message, e);
  }
  public ErrorPacketImpl errorPacket(String message) {
    logger.error(message);
    int defaultErrorCode = ER_UNKNOWN_ERROR;
    return getErrorPacket(message, defaultErrorCode);
  }

  public ErrorPacketImpl getErrorPacket(String message, int defaultErrorCode) {
    ErrorPacketImpl errorPacket = new ErrorPacketImpl();
    errorPacket.setErrorCode(defaultErrorCode);
    errorPacket.setErrorMessage(message.getBytes());
    return errorPacket;
  }


  public ErrorPacketImpl errorPacket(int error, String s, Object o) {
    FormattingTuple ft = MessageFormatter.format(s, o);
    logger.error(ft.getMessage(), ft.getThrowable());
    return getErrorPacket(ft.getMessage(), error);
  }

  public ErrorPacketImpl errorPacket(int error, String s, Object o, Object o1) {
    FormattingTuple ft = MessageFormatter.format(s, o, o1);
    logger.error(ft.getMessage(), ft.getThrowable());
    return getErrorPacket(ft.getMessage(), error);
  }

  public ErrorPacketImpl errorPacket(int error, String s, Object... objects) {
    FormattingTuple ft = MessageFormatter.format(s, objects);
    ErrorPacketImpl errorPacket = getErrorPacket(ft.getMessage(), error);
    logger.error(ft.getMessage(), ft.getThrowable());
    return errorPacket;
  }

  public ErrorPacketImpl errorPacket(String s, Throwable throwable) {
    FormattingTuple ft = MessageFormatter.format(s, throwable);
    ErrorPacketImpl errorPacket = getErrorPacket(ft.getMessage(), ER_UNKNOWN_ERROR);
    logger.error(ft.getMessage(), ft.getThrowable());
    return errorPacket;
  }

  public boolean isErrorEnabled(Marker marker) {
    return logger.isErrorEnabled(marker);
  }

  public void error(Marker marker, String s) {
    logger.error(marker, s);
  }

  public void error(Marker marker, String s, Object o) {
    logger.error(marker, s, o);
  }

  public void error(Marker marker, String s, Object o, Object o1) {
    logger.error(marker, s, o, o1);
  }

  public void error(Marker marker, String s, Object... objects) {
    logger.error(marker, s, objects);
  }

  public void error(Marker marker, String s, Throwable throwable) {
    logger.error(marker, s, throwable);
  }

}
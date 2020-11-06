/**
 * Copyright (C) <2020>  <chenjunwen>
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
package io.mycat.command;


import io.mycat.proxy.session.MycatSession;

/**
 * @author jamie12221
 *  date 2019-05-12 21:00
 * mysql server 对LocalData 的处理
 **/
public interface LocalInFileRequestParseHelper {

  void handleQuery(byte[] sql, MycatSession seesion) throws Exception;

  void handleContentOfFilename(byte[] sql, MycatSession session);

  void handleContentOfFilenameEmptyOk(MycatSession session);

  interface LocalInFileSession {

    boolean shouldHandleContentOfFilename();

    void setHandleContentOfFilename(boolean need);
  }

}

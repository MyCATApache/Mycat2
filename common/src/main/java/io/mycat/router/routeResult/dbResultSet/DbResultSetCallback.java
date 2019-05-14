/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.router.routeResult.dbResultSet;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author jamie12221
 * @date 2019-05-05 23:51
 * 复杂sql处理回调
 **/
public interface DbResultSetCallback<CONTEXT extends Serializable> {
  void onStart(CONTEXT attr);
  void onWrite(ByteBuffer buffer,CONTEXT attr);
  void onFinished(CONTEXT attr);
  void onTimeOut(Throwable throwable,CONTEXT attr);
}

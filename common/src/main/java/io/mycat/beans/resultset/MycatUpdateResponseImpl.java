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
package io.mycat.beans.resultset;

import java.io.IOException;

public class MycatUpdateResponseImpl implements MycatUpdateResponse {

  final int updateCount;
  final long lastInsertId;
  final int serverstatus;

  public MycatUpdateResponseImpl(int updateCount, long lastInsertId,int serverstatus) {
    this.updateCount = updateCount;
    this.lastInsertId = lastInsertId;
    this.serverstatus = serverstatus;
  }

  @Override
  public int getUpdateCount() {
    return updateCount;
  }

  @Override
  public long getLastInsertId() {
    return lastInsertId;
  }

  @Override
  public int serverStatus() {
    return serverstatus;
  }

  @Override
  public void close() throws IOException {

  }
}
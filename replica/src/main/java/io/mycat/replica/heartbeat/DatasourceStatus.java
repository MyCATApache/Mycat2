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
package io.mycat.replica.heartbeat;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static io.mycat.replica.heartbeat.DatasourceEnum.DB_SYN_NORMAL;
import static io.mycat.replica.heartbeat.DatasourceEnum.OK_STATUS;

/**
 * @author : zhangwy
 * @version V1.0
 *
 *  date Date : 2019年05月05日 0:05
 */
@EqualsAndHashCode
@Data
@ToString
public class DatasourceStatus {
  // heartbeat config
  private DatasourceEnum status = OK_STATUS; //心跳状态
  private boolean isSlaveBehindMaster = false; //同步延时
  private DatasourceEnum dbSynStatus = DB_SYN_NORMAL; //同步状态

  public DatasourceStatus() {
  }

  public boolean isError() {
    return !isAlive();
  }

  public boolean isAlive() {
    return status == OK_STATUS;
  }


  public boolean isDbSynStatusNormal() {
    return dbSynStatus == DB_SYN_NORMAL;
  }

}

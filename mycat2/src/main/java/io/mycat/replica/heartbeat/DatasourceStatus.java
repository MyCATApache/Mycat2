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
package io.mycat.replica.heartbeat;

/**
 * @author : zhangwy
 * @version V1.0
 *
 *  date Date : 2019年05月05日 0:05
 */
public class DatasourceStatus {

    public static final int DB_SYN_ERROR = -1;
    public static final int DB_SYN_NORMAL = 1;

    public static final int OK_STATUS = 1;
    public static final int ERROR_STATUS = -1;
    public static final int TIMEOUT_STATUS = -2;
    public static final int INIT_STATUS = 0;

    // heartbeat config
    private int status = OK_STATUS; //心跳状态
    private boolean isSlaveBehindMaster = false ; //同步延时
    private int dbSynStatus = DB_SYN_NORMAL; //同步状态

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public boolean isSlaveBehindMaster() {
        return isSlaveBehindMaster;
    }

    public void setSlaveBehindMaster(boolean slaveBehindMaster) {
        isSlaveBehindMaster = slaveBehindMaster;
    }

    public int getDbSynStatus() {
        return dbSynStatus;
    }

    public void setDbSynStatus(int dbSynStatus) {
        this.dbSynStatus = dbSynStatus;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasourceStatus that = (DatasourceStatus) o;

        if (status != that.status) return false;
        if (isSlaveBehindMaster != that.isSlaveBehindMaster) return false;
        return dbSynStatus == that.dbSynStatus;
    }

    @Override
    public int hashCode() {
        int result = status;
        result = 31 * result + (isSlaveBehindMaster ? 1 : 0);
        result = 31 * result + dbSynStatus;
        return result;
    }

    public boolean isError() {
        return !isAlive();
    }
    public boolean isAlive(){
        return status == OK_STATUS;
    }

    @Override
    public String toString() {
        return "DatasourceStatus{" +
            "status=" + status +
            ", isSlaveBehindMaster=" + isSlaveBehindMaster +
            ", dbSynStatus=" + dbSynStatus +
            '}';
    }
}
